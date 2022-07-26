package env

import (
	"fmt"
	"os"

	"github.com/kardianos/service"
)

type Service struct {
	Config   *service.Config
	Startup  []func()
	Shutdown []func()
	Init     func()
	Test     func()
}

func (p *Service) AddStart(fn func()) {
	p.Startup = append(p.Startup, fn)
}

func (p *Service) AddStop(fn func()) {
	p.Shutdown = append(p.Shutdown, fn)
}

func (p *Service) Start(s service.Service) error {
	for _, fn := range p.Startup {
		go fn()
	}
	return nil
}

func (p *Service) Stop(s service.Service) error {
	for _, fn := range p.Shutdown {
		go fn()
	}
	return nil
}

func (p *Service) Run(s service.Service) error {
	p.Start(s)
	select {}
}

var YTSN = &Service{
	Config: &service.Config{
		Name:        "ytsnd",
		DisplayName: "go ytsn service",
		Description: "go ytsn daemons service",
	},
}

var YTS3 = &Service{
	Config: &service.Config{
		Name:        "yts3",
		DisplayName: "go yts3 service",
		Description: "go yts3 daemons service",
	},
}

func LaunchYTSN() {
	launch(YTSN)
}

func LaunchYTS3() {
	launch(YTS3)
}

func launch(srv *Service) {
	s, err := service.New(srv, srv.Config)
	if err != nil {
		panic(err)
	}
	if len(os.Args) > 1 {
		cmd := os.Args[1]
		if cmd == "init" {
			if srv.Init != nil {
				srv.Init()
			}
			return
		}
		if cmd == "test" {
			if srv.Test != nil {
				srv.Test()
			}
			return
		}
		if cmd == "console" {
			Console = true
			err = s.Run()
			if err != nil {
				fmt.Println("Run err:", err.Error())
			}
			return
		}
		if cmd == "start" {
			err = s.Start()
			if err != nil {
				fmt.Println("Maybe the daemons are not installed.Start err:", err.Error())
			} else {
				fmt.Println("Start OK.")
			}
			return
		}
		if cmd == "restart" {
			err = s.Restart()
			if err != nil {
				fmt.Println("Maybe the daemons are not installed.Restart err:", err.Error())
			} else {
				fmt.Println("Restart OK.")
			}
			return
		}
		if cmd == "stop" {
			err = s.Stop()
			if err != nil {
				fmt.Println("Stop err:", err.Error())
			} else {
				fmt.Println("Stop OK.")
			}
			return
		}
		if cmd == "install" {
			err = s.Install()
			if err != nil {
				fmt.Println("Install err:", err.Error())
			} else {
				fmt.Println("Install OK.")
			}
			return
		}
		if cmd == "uninstall" {
			err = s.Uninstall()
			if err != nil {
				fmt.Println("Uninstall err:", err.Error())
			} else {
				fmt.Println("Uninstall OK.")
			}
			return
		}
		fmt.Println("Commands:")
		fmt.Println("version      Show versionid.")
		fmt.Println("console      Launch in the current console.")
		fmt.Println("start        Start in the background as a daemon process.")
		fmt.Println("stop         Stop if running as a daemon or in another console.")
		fmt.Println("restart      Restart if running as a daemon or in another console.")
		fmt.Println("install      Install to start automatically when system boots.")
		fmt.Println("uninstall    Uninstall.")
		return
	}
	err = s.Run()
	if err != nil {
		fmt.Println("Run err:", err.Error())
	}
}
