package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, terminalText(err.Error(), 500))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	command := "watch"
	if len(args) > 0 {
		command, args = args[0], args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	duration, interval, threadLookback := 24*time.Hour, 15*time.Second, 24*time.Hour
	var end string
	switch command {
	case "login":
	case "stats":
		flags.DurationVar(&duration, "since", duration, "统计时段，例如 2h 或 168h")
		flags.StringVar(&end, "until", "", "统计结束时间（RFC3339，默认现在）")
	case "watch":
		flags.DurationVar(&interval, "interval", interval, "两轮开始之间的最短间隔")
		flags.DurationVar(&threadLookback, "thread-lookback", threadLookback, "监听时发现既有群话题的回看时长")
	case "help", "--help", "-h":
		fmt.Println("用法: feishu-im-watch login | stats [--since 24h] [--until RFC3339] | watch [--interval 15s] [--thread-lookback 24h]")
		return nil
	default:
		return fmt.Errorf("未知命令 %q，请运行 feishu-im-watch --help", command)
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || duration <= 0 || interval < time.Second || threadLookback <= 0 {
		return errors.New("参数无效：统计时段与话题回看时长必须大于 0，轮询间隔至少 1s")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	httpClient := &http.Client{
		Transport: transport, Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	auth, err := newAuthStore(httpClient)
	if err != nil {
		return err
	}
	if command == "login" {
		if err := auth.login(ctx, func(link, code string) {
			fmt.Printf("请在浏览器打开此链接，并用飞书确认授权：\n%s\n验证码 %s\n", link, code)
		}); err != nil {
			return err
		}
		fmt.Println("飞书用户授权已保存")
		return nil
	}
	client := newIMClient(httpClient, auth.credentials)
	client.token, err = auth.accessToken(ctx)
	if err != nil {
		return err
	}
	if command == "stats" {
		until := time.Now()
		if end != "" {
			until, err = time.Parse(time.RFC3339, end)
			if err != nil {
				return fmt.Errorf("--until 需要 RFC3339 时间，例如 2026-11-25T10:13:28+08:00: %w", err)
			}
		}
		return printStats(ctx, client, duration, until)
	}
	return watch(ctx, client, auth, interval, threadLookback)
}
