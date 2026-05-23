package sip

import (
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/require"

	"github.com/livekit/sip/pkg/stats"
)

func TestTerminationFromRoomDisconnect(t *testing.T) {
	tests := []struct {
		name   string
		reason livekit.DisconnectReason
		want   stats.Termination
	}{
		{name: "client initiated", reason: livekit.DisconnectReason_CLIENT_INITIATED, want: stats.Success("removed")},
		{name: "participant removed", reason: livekit.DisconnectReason_PARTICIPANT_REMOVED, want: stats.Success("removed")},
		{name: "room closed", reason: livekit.DisconnectReason_ROOM_CLOSED, want: stats.Success("removed")},
		{name: "join failure", reason: livekit.DisconnectReason_JOIN_FAILURE, want: stats.ServerError("room-failed")},
		{name: "user unavailable", reason: livekit.DisconnectReason_USER_UNAVAILABLE, want: stats.ClientError("user-unavailable")},
		{name: "unknown", reason: livekit.DisconnectReason_UNKNOWN_REASON, want: stats.ServerError("room-disconnected")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, terminationFromRoomDisconnect(tt.reason))
		})
	}
}
