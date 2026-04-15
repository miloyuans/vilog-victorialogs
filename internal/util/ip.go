package util

import (
	"net"
	"net/http"
	"strings"
)

type IPMatcher struct {
	ips  []net.IP
	nets []*net.IPNet
}

func ParseIPMatcher(entries []string) (*IPMatcher, error) {
	matcher := &IPMatcher{
		ips:  make([]net.IP, 0, len(entries)),
		nets: make([]*net.IPNet, 0, len(entries)),
	}

	for _, entry := range entries {
		trimmed := strings.TrimSpace(entry)
		if trimmed == "" {
			continue
		}
		if ip := net.ParseIP(trimmed); ip != nil {
			matcher.ips = append(matcher.ips, ip)
			continue
		}
		if _, network, err := net.ParseCIDR(trimmed); err == nil {
			matcher.nets = append(matcher.nets, network)
			continue
		}
		return nil, &net.ParseError{Type: "IP/CIDR", Text: trimmed}
	}

	return matcher, nil
}

func (m *IPMatcher) Empty() bool {
	return m == nil || (len(m.ips) == 0 && len(m.nets) == 0)
}

func (m *IPMatcher) Contains(ip net.IP) bool {
	if m == nil || ip == nil {
		return false
	}
	for _, candidate := range m.ips {
		if candidate.Equal(ip) {
			return true
		}
	}
	for _, network := range m.nets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func ExtractClientIP(r *http.Request, trustProxyHeaders bool) net.IP {
	if trustProxyHeaders {
		if ip := firstValidIP(r.Header.Get("X-Forwarded-For")); ip != nil {
			return ip
		}
		if ip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); ip != nil {
			return ip
		}
	}

	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip
		}
	}

	return net.ParseIP(strings.TrimSpace(r.RemoteAddr))
}

func firstValidIP(xff string) net.IP {
	parts := strings.Split(xff, ",")
	for _, part := range parts {
		if ip := net.ParseIP(strings.TrimSpace(part)); ip != nil {
			return ip
		}
	}
	return nil
}
