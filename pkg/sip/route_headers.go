// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sip

import (
	"strings"

	"github.com/livekit/sipgo/sip"
)

func cloneRouteHeaders(routes []string) []string {
	if len(routes) == 0 {
		return nil
	}
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		route = strings.TrimSpace(route)
		if route != "" {
			out = append(out, route)
		}
	}
	return out
}

func appendRouteHeaders(routes ...[]string) []string {
	var out []string
	for _, set := range routes {
		out = append(out, cloneRouteHeaders(set)...)
	}
	return out
}

func prependRouteHeaders(req *sip.Request, routes []string) {
	for i := len(routes) - 1; i >= 0; i-- {
		req.PrependHeader(sip.NewHeader("Route", routes[i]))
	}
}

func serviceRouteHeaders(resp *sip.Response) []string {
	if resp == nil {
		return nil
	}
	var routes []string
	for _, h := range resp.GetHeaders("Service-Route") {
		if h == nil {
			continue
		}
		value := strings.TrimSpace(h.Value())
		if value != "" {
			routes = append(routes, value)
		}
	}
	return routes
}

func registrationRouteHeader(conf *ResolvedRegistrationConfig) string {
	if conf == nil {
		return ""
	}
	uri := conf.Registrar
	if conf.RouteRegistrar.GetHost() != "" {
		uri = conf.RouteRegistrar
	}
	return "<" + uri.GetURI().String() + ";lr>"
}
