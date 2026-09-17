package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"maunium.net/go/mautrix"
)

// TODO move proxy stuff to mautrix-go

type respGetProxy struct {
	ProxyURL string `json:"proxy_url"`
}

func (wa *WhatsAppConnector) getProxy(ctx context.Context, reason string) (string, error) {
	if wa.Config.GetProxyURL == "" {
		return wa.Config.Proxy, nil
	}
	parsed, err := url.Parse(wa.Config.GetProxyURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse address: %w", err)
	}
	q := parsed.Query()
	q.Set("reason", reason)
	parsed.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to prepare request: %w", err)
	}
	req.Header.Set("User-Agent", mautrix.DefaultUserAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	// Always close the response body and return it to the connection pool;
	// failing to do so leaks a connection for every proxy fetch.
	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return "", fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	var respData respGetProxy
	err = json.NewDecoder(resp.Body).Decode(&respData)
	if err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	return respData.ProxyURL, nil
}

func (wa *WhatsAppConnector) updateProxy(ctx context.Context, client *whatsmeow.Client, isLogin bool) error {
	if wa.Config.ProxyOnlyLogin && !isLogin {
		return nil
	}
	reason := "connect"
	if isLogin {
		reason = "login"
	}
	// The proxy fetch runs independently of the caller's context (which may be
	// cancelled mid-login/connect), but is bounded by its own timeout so a
	// stalled proxy endpoint cannot hang the caller forever.
	proxyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if proxy, err := wa.getProxy(proxyCtx, reason); err != nil {
		return fmt.Errorf("failed to get proxy address: %w", err)
	} else if proxy == "" {
		return nil
	} else if err = client.SetProxyAddress(proxy, whatsmeow.SetProxyOptions{
		OnlyLogin: wa.Config.ProxyOnlyLogin,
		NoMedia:   wa.Config.ProxyOnlyLogin,
	}); err != nil {
		return fmt.Errorf("failed to set proxy address: %w", err)
	}
	zerolog.Ctx(ctx).Debug().Msg("Enabled proxy")
	return nil
}
