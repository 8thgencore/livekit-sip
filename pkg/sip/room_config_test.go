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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/sip/pkg/config"
)

func TestCreateRoomRequestFromConfig(t *testing.T) {
	rconf := RoomConfig{
		RoomName:   "room",
		RoomPreset: "preset",
		RoomConfig: &livekit.RoomConfiguration{
			EmptyTimeout:     30,
			DepartureTimeout: 10,
			MaxParticipants:  5,
			Metadata:         "room-metadata",
			MinPlayoutDelay:  100,
			MaxPlayoutDelay:  200,
			SyncStreams:      true,
			Agents: []*livekit.RoomAgentDispatch{
				{
					AgentName: "agent",
					Metadata:  "agent-metadata",
				},
			},
		},
	}

	req := createRoomRequestFromConfig(rconf)

	require.Equal(t, "room", req.Name)
	require.Equal(t, "preset", req.RoomPreset)
	require.Equal(t, uint32(30), req.EmptyTimeout)
	require.Equal(t, uint32(10), req.DepartureTimeout)
	require.Equal(t, uint32(5), req.MaxParticipants)
	require.Equal(t, "room-metadata", req.Metadata)
	require.Equal(t, uint32(100), req.MinPlayoutDelay)
	require.Equal(t, uint32(200), req.MaxPlayoutDelay)
	require.True(t, req.SyncStreams)
	require.Len(t, req.Agents, 1)
	require.Equal(t, "agent", req.Agents[0].AgentName)
	require.Equal(t, "agent-metadata", req.Agents[0].Metadata)
}

func TestBuildSIPJoinTokenOmitsRoomConfiguration(t *testing.T) {
	const (
		apiKey    = "api-key"
		apiSecret = "secret"
	)
	attrs := map[string]string{
		livekit.AttrSIPCallID: "call-id",
	}
	token, err := buildSIPJoinToken(
		&config.Config{
			ApiKey:    apiKey,
			ApiSecret: apiSecret,
		},
		RoomConfig{
			RoomName:   "room",
			RoomPreset: "large-preset",
			RoomConfig: &livekit.RoomConfiguration{
				Agents: []*livekit.RoomAgentDispatch{
					{
						AgentName: "agent",
						Metadata:  "large-agent-metadata",
					},
				},
			},
		},
		ParticipantConfig{
			Identity: "sip-user",
			Name:     "SIP User",
			Metadata: "participant-metadata",
		},
		attrs,
	)
	require.NoError(t, err)

	verifier, err := auth.ParseAPIToken(token)
	require.NoError(t, err)
	_, grants, err := verifier.Verify(apiSecret)
	require.NoError(t, err)

	require.Equal(t, "sip-user", grants.Identity)
	require.Equal(t, "SIP User", grants.Name)
	require.Equal(t, "participant-metadata", grants.Metadata)
	require.Equal(t, attrs, grants.Attributes)
	require.Empty(t, grants.RoomPreset)
	require.Nil(t, grants.RoomConfig)
	require.NotNil(t, grants.Video)
	require.True(t, grants.Video.RoomJoin)
	require.Equal(t, "room", grants.Video.Room)
}
