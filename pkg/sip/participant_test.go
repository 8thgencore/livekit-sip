package sip

import (
	"testing"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/livekit/sip/pkg/stats"
)

func TestTerminationFromRoomDisconnect(t *testing.T) {
	tests := []struct {
		name   string
		reason lksdk.DisconnectionReason
		want   stats.Termination
	}{
		{name: "leave requested", reason: lksdk.LeaveRequested, want: stats.Success("removed")},
		{name: "participant removed", reason: lksdk.ParticipantRemoved, want: stats.Success("removed")},
		{name: "room closed", reason: lksdk.RoomClosed, want: stats.ServerError("lk-room-ended")},
		{name: "other reason", reason: lksdk.OtherReason, want: stats.ServerError("lk-room-ended")},
		{name: "failed", reason: lksdk.Failed, want: stats.ServerError("room-failed")},
		{name: "user unavailable", reason: lksdk.UserUnavailable, want: stats.ServerError("lk-room-ended")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, terminationFromRoomDisconnect(tt.reason))
		})
	}
}
