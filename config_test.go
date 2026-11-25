package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFeishuConfigPrefersNewFilename(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	dir := filepath.Join(configHome, "feishu-im-watch")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	writeTestConfig(t, dir, "credentials.json", `{"app_id":"cli_legacy","app_secret":"legacy-secret"}`)
	store, err := newAuthStore(nil)
	if err != nil || store.AppID != "cli_legacy" {
		t.Fatalf("legacy Feishu config was not loaded: %v", err)
	}
	writeTestConfig(t, dir, "feishu.json", `{"app_id":"cli_current","app_secret":"current-secret"}`)
	store, err = newAuthStore(nil)
	if err != nil || store.AppID != "cli_current" {
		t.Fatalf("canonical Feishu config was not loaded: %v", err)
	}
}

func TestOpenVikingConfigPrefersJSONGroupList(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OPENVIKING_API_KEY", "legacy-key")
	t.Setenv("OPENVIKING_AGENT_ID", "")
	writeTestConfig(t, dir, "openviking-groups.txt", "oc_legacy\n")
	config, groups, err := loadOpenVikingConfig(dir)
	if err != nil || config.Endpoint != defaultOpenVikingEndpoint || !groups["oc_legacy"] {
		t.Fatalf("legacy OpenViking configuration was not loaded: %v", err)
	}
	writeTestConfig(t, dir, "openviking.json", `{
		"endpoint": "https://example.com/openviking/",
		"api_key": "file-key",
		"agent_id": "configured-agent",
		"group_ids": ["oc_selected"]
	}`)
	config, groups, err = loadOpenVikingConfig(dir)
	if err != nil || config.Endpoint != "https://example.com/openviking" || config.APIKey != "legacy-key" ||
		config.AgentID != "configured-agent" || len(groups) != 1 || !groups["oc_selected"] {
		t.Fatalf("canonical OpenViking config or environment override was not loaded: %v", err)
	}
	t.Setenv("OPENVIKING_API_KEY", "")
	config, _, err = loadOpenVikingConfig(dir)
	if err != nil || config.APIKey != "file-key" {
		t.Fatalf("OpenViking key was not loaded from JSON: %v", err)
	}
}

func writeTestConfig(t *testing.T, dir, filename, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
