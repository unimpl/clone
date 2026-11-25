package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const accountsURL = "https://accounts.feishu.cn"
const imScopes = "im:chat:read im:message:readonly im:message.p2p_msg:get_as_user im:message.group_msg:get_as_user im:feed.shortcut:read offline_access"

type credentials struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type session struct {
	AppID        string    `json:"app_id"`
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type oauthReply struct {
	Code                    int    `json:"code"`
	ErrorName               string `json:"error"`
	ErrorDescription        string `json:"error_description"`
	Message                 string `json:"msg"`
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	Interval                int    `json:"interval"`
	ExpiresIn               int    `json:"expires_in"`
	AccessToken             string `json:"access_token"`
	RefreshToken            string `json:"refresh_token"`
}

type authStore struct {
	credentials
	dir  string
	http *http.Client
}

func newAuthStore(client *http.Client) (authStore, error) {
	config, err := os.UserConfigDir()
	if err != nil {
		return authStore{}, err
	}
	store := authStore{dir: filepath.Join(config, "feishu-im-watch"), http: client}
	path := filepath.Join(store.dir, "feishu.json")
	err = readPrivateJSON(path, &store.credentials)
	if errors.Is(err, os.ErrNotExist) {
		legacyPath := filepath.Join(store.dir, "credentials.json")
		if legacyErr := readPrivateJSON(legacyPath, &store.credentials); !errors.Is(legacyErr, os.ErrNotExist) {
			path, err = legacyPath, legacyErr
		}
	}
	if err != nil {
		return authStore{}, fmt.Errorf("读取应用配置 %s: %w", path, err)
	}
	if store.AppID == "" || store.AppSecret == "" {
		return authStore{}, errors.New("应用配置需要 app_id 和 app_secret")
	}
	return store, nil
}

func readPrivateJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return errors.New("凭证文件必须是普通文件，且仅当前用户可读写（chmod 600）")
	}
	return json.NewDecoder(file).Decode(target)
}

func (store authStore) request(ctx context.Context, path string, values url.Values, basicAuth bool) (oauthReply, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, accountsURL+path, strings.NewReader(values.Encode()))
	if err != nil {
		return oauthReply{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if basicAuth {
		request.SetBasicAuth(store.AppID, store.AppSecret)
	}
	response, err := store.http.Do(request)
	if err != nil {
		return oauthReply{}, err
	}
	defer response.Body.Close()
	var reply oauthReply
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&reply); err != nil {
		return oauthReply{}, fmt.Errorf("飞书 OAuth HTTP %d 响应无法解析: %w", response.StatusCode, err)
	}
	if reply.ErrorName != "" || reply.Code != 0 || response.StatusCode >= 300 {
		return reply, fmt.Errorf("飞书 OAuth HTTP %d，code=%d %s %s %s", response.StatusCode, reply.Code, reply.ErrorName, reply.ErrorDescription, reply.Message)
	}
	return reply, nil
}

func (store authStore) login(ctx context.Context, showCode func(string, string)) error {
	device, err := store.request(ctx, "/oauth/v1/device_authorization", url.Values{
		"client_id": {store.AppID}, "scope": {imScopes},
	}, true)
	if err != nil {
		return fmt.Errorf("申请设备授权: %w", err)
	}
	verificationURL := device.VerificationURIComplete
	if verificationURL == "" {
		verificationURL = device.VerificationURI
	}
	if device.DeviceCode == "" || verificationURL == "" {
		return errors.New("飞书没有返回有效的设备授权信息")
	}
	showCode(verificationURL, device.UserCode)
	if device.Interval < 1 {
		device.Interval = 5
	}
	if device.ExpiresIn < 1 {
		device.ExpiresIn = 240
	}
	pollContext, cancel := context.WithTimeout(ctx, time.Duration(device.ExpiresIn)*time.Second)
	defer cancel()
	interval := time.Duration(device.Interval) * time.Second
	values := url.Values{
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"device_code": {device.DeviceCode}, "client_id": {store.AppID}, "client_secret": {store.AppSecret},
	}
	for {
		select {
		case <-pollContext.Done():
			return fmt.Errorf("等待飞书授权: %w", pollContext.Err())
		case <-time.After(interval):
		}
		reply, err := store.request(pollContext, "/oauth/v3/token", values, false)
		if err == nil {
			return store.withSessionLock(func() error { return store.saveSession(reply, "") })
		}
		switch reply.ErrorName {
		case "authorization_pending":
		case "slow_down":
			interval += 5 * time.Second
		default:
			return fmt.Errorf("完成设备授权: %w", err)
		}
	}
}

func (store authStore) withSessionLock(action func() error) error {
	lock, err := os.OpenFile(filepath.Join(store.dir, "session.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	return action()
}

func (store authStore) saveSession(reply oauthReply, previousRefreshToken string) error {
	if reply.AccessToken == "" || reply.ExpiresIn <= 0 {
		return errors.New("飞书没有返回有效的用户访问令牌")
	}
	if reply.RefreshToken == "" {
		reply.RefreshToken = previousRefreshToken
	}
	data := session{AppID: store.AppID, AccessToken: reply.AccessToken, RefreshToken: reply.RefreshToken,
		ExpiresAt: time.Now().Add(time.Duration(reply.ExpiresIn) * time.Second)}
	file, err := os.CreateTemp(store.dir, ".session-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encodeErr := json.NewEncoder(file).Encode(data)
	closeErr := file.Close()
	if err := errors.Join(encodeErr, closeErr); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(store.dir, "session.json"))
}

func (store authStore) accessToken(ctx context.Context) (string, error) {
	var token string
	err := store.withSessionLock(func() error {
		var cached session
		if err := readPrivateJSON(filepath.Join(store.dir, "session.json"), &cached); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return errors.New("请先运行 feishu-im-watch login 完成用户授权")
			}
			return err
		}
		if cached.AppID != store.AppID {
			return errors.New("应用已变更，请运行 feishu-im-watch login 重新授权")
		}
		if cached.AccessToken != "" && time.Until(cached.ExpiresAt) > time.Minute {
			token = cached.AccessToken
			return nil
		}
		if cached.RefreshToken == "" {
			return errors.New("缺少刷新令牌，请检查 offline_access 和应用安全设置，再运行 feishu-im-watch login")
		}
		reply, err := store.request(ctx, "/oauth/v3/token", url.Values{
			"grant_type": {"refresh_token"}, "refresh_token": {cached.RefreshToken},
			"client_id": {store.AppID}, "client_secret": {store.AppSecret},
		}, false)
		if err != nil {
			return fmt.Errorf("刷新用户授权: %w", err)
		}
		if err := store.saveSession(reply, cached.RefreshToken); err != nil {
			return err
		}
		token = reply.AccessToken
		return nil
	})
	return token, err
}
