package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/OneOfOne/xxhash"
	"github.com/fsnotify/fsnotify"
	hplugin "github.com/hashicorp/go-plugin"
	json "github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	_ "github.com/moiot/gravity/pkg/env"
	"github.com/moiot/gravity/pkg/logutil"
	"github.com/moiot/gravity/pkg/utils"
)

var myJson = json.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func main() {
	// 初始化命令行参数
	cfg := config.NewConfig()
	err := cfg.ParseCmd(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags errors: %s\n", err)
	}

	// 对阻塞事件采样, <=0则不采样
	runtime.SetBlockProfileRate(cfg.BlockProfileRate)
	// pprof查询互斥锁对象的不开启 0
	runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)

	if cfg.Version {
		utils.PrintRawInfo("gravity")
		os.Exit(0)
	}

	log.Info("xxhash backend: ", xxhash.Backend)

	if cfg.ConfigFile != "" {
		// 读取命令行指定配置
		if err := cfg.ConfigFromFile(cfg.ConfigFile); err != nil {
			log.Fatalf("failed to load config from file: %v", errors.ErrorStack(err))
		}
	} else {
		log.Fatal("config must not be empty")
	}

	content, err := ioutil.ReadFile(cfg.ConfigFile)
	if err != nil {
		log.Fatalf("fail to read file %s. err: %s", cfg.ConfigFile, err)
	}
	hash := core.HashConfig(string(content))

	logutil.MustInitLogger(&cfg.Log)

	//输出欢迎及状态信息
	utils.LogRawInfo("gravity")

	logutil.PipelineName = cfg.PipelineConfig.PipelineName
	consts.GravityDBName = cfg.PipelineConfig.InternalDBName

	// 注册退出处理handler
	log.RegisterExitHandler(func() {
		hplugin.CleanupClients()
	})

	server, err := app.NewServer(cfg.PipelineConfig)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	//如果配置清除binlog位点,则清除server中位点缓存
	if cfg.ClearPosition {
		if err := server.PositionCache.Clear(); err != nil {
			log.Errorf("failed to clear position, err: %v", errors.ErrorStack(err))
		}
		return
	}

	// 启动协程web server，便于获取运行状态
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/reset", resetHandler(server, cfg.PipelineConfig))
		http.HandleFunc("/status", statusHandler(server, cfg.PipelineConfig.PipelineName, hash))
		http.HandleFunc("/healthz", healthzHandler(server))
		err = http.ListenAndServe(cfg.HttpAddr, nil)
		if err != nil {
			log.Fatalf("http error: %v", err)
		}
		log.Info("starting http server")
	}()

	// 启动工作server
	err = server.Start()
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	// 创建监听器
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(errors.Trace(err))
	}
	defer watcher.Close()
	// 监听配置文件变化
	err = watcher.Add(cfg.ConfigFile)
	if err != nil {
		log.Fatal(errors.Trace(err))
	}

	//创建进程信号channel对象
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// When deploying with k8s cluster,
	// batch mode won't continue if the return code of this process is 0.
	// This process exit with 0 only when the input is done in batch mode.
	// For all the other cases, this process exit with 1.
	//对于批处理模式下.k8s,在等待输入完成时才以0退出进程
	if cfg.PipelineConfig.InputPlugin.Mode == config.Batch {
		go func(server *app.Server) {
			<-server.Input.Done()
			server.Close()
			os.Exit(0)
		}(server)
	}

	for {
		select {
		case sig := <-sc:
			log.Infof("[gravity] stop with signal %v", sig)
			server.Close()
			os.Exit(1)

		case event, ok := <-watcher.Events:
			if !ok {
				continue
			}
			if event.Name != cfg.ConfigFile {
				continue
			}

			log.Info("config file event: ", event.String())

			content, err := ioutil.ReadFile(cfg.ConfigFile)
			if err != nil {
				log.Infof("read config error: %s", err)
			} else {
				newHash := core.HashConfig(string(content))
				if newHash == hash {
					log.Infof("config not changed")
					continue
				}
			}

			log.Info("config file updated, quit...")
			server.Close()
			os.Exit(1)

		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			log.Println("error:", err)
			server.Close()
			os.Exit(1)
		}
	}
}

func healthzHandler(server *app.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if server.Scheduler.Healthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func statusHandler(server *app.Server, name, hash string) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		position, v, exist, err := server.PositionCache.GetEncodedPersistentPosition()
		if err != nil || !exist {
			writer.WriteHeader(http.StatusInternalServerError)
			log.Errorf("[statusHandler] failed to get positionRepoModel, exist: %v, err: %v", exist, errors.ErrorStack(err))
			return
		}

		var state = core.ReportStageIncremental
		if position.Stage == config.Batch {
			state = core.ReportStageFull
		}

		ret := core.TaskReportStatus{
			Name:       name,
			ConfigHash: hash,
			Position:   v,
			Stage:      state,
			Version:    utils.Version,
		}

		resp, err := myJson.Marshal(ret)
		if err != nil {
			log.Error(err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(resp)
	}
}

func resetHandler(server *app.Server, _ config.PipelineConfigV3) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		if err := server.PositionCache.Clear(); err != nil {
			log.Errorf("[reset] failed to clear position, err: %v", errors.ErrorStack(err))
			http.Error(writer, fmt.Sprintf("failed to clear position, err: %v", errors.ErrorStack(err)), 500)
			return
		}
		server.Close()
		os.Exit(1)
	}
}
