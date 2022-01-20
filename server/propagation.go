package server

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

// getPropagateInbox returns the NATS subject used for handling propagated Raft
// operations. The server subscribes to this when it is the metadata leader.
// Followers can then forward operations for the leader to apply.
func (s *Server) getPropagateInbox() string {
	return fmt.Sprintf("%s.propagate", s.baseMetadataRaftSubject())
}

// handlePropagatedRequest is a NATS handler used to process propagated Raft
// operations from followers in the Raft cluster. This is activated when the
// server becomes the metadata leader. If, for some reason, the server receives
// a forwarded operation and loses leadership at the same time, the operation
// will fail when it's proposed to the Raft cluster.
func (s *Server) handlePropagatedRequest(m *nats.Msg) {
	var (
		req, err = proto.UnmarshalPropagatedRequest(m.Data)
		resp     *proto.PropagatedResponse
	)
	if err != nil {
		s.logger.Warnf("Invalid propagated request: %v", err)
		return
	}
	switch req.Op {
	case proto.Op_CREATE_STREAM:
		resp = s.handleCreateStream(req)
	case proto.Op_SHRINK_ISR:
		resp = s.handleShrinkISR(req)
	case proto.Op_EXPAND_ISR:
		resp = s.handleExpandISR(req)
	case proto.Op_REPORT_LEADER:
		resp = s.handleReportLeader(req)
	case proto.Op_DELETE_STREAM:
		resp = s.handleDeleteStream(req)
	case proto.Op_PAUSE_STREAM:
		resp = s.handlePauseStream(req)
	case proto.Op_RESUME_STREAM:
		resp = s.handleResumeStream(req)
	case proto.Op_SET_STREAM_READONLY:
		resp = s.handleSetStreamReadonly(req)
	case proto.Op_JOIN_CONSUMER_GROUP:
		resp = s.handleJoinConsumerGroup(req)
	case proto.Op_LEAVE_CONSUMER_GROUP:
		resp = s.handleLeaveConsumerGroup(req)
	case proto.Op_REPORT_CONSUMER_GROUP_COORDINATOR:
		resp = s.handleReportConsumerGroupCoordinator(req)
	default:
		s.logger.Warnf("Unknown propagated request operation: %s", req.Op)
		return
	}
	data, err := proto.MarshalPropagatedResponse(resp)
	if err != nil {
		panic(err)
	}
	if err := m.Respond(data); err != nil {
		s.logger.Errorf("Failed to respond to propagated request: %v", err)
	}
}

func (s *Server) handleCreateStream(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.CreateStream(context.Background(), req.CreateStreamOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleShrinkISR(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.ShrinkISR(context.Background(), req.ShrinkISROp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleExpandISR(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.ExpandISR(context.Background(), req.ExpandISROp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleReportLeader(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.ReportLeader(context.Background(), req.ReportLeaderOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleDeleteStream(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.DeleteStream(context.Background(), req.DeleteStreamOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handlePauseStream(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.PauseStream(context.Background(), req.PauseStreamOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleResumeStream(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.ResumeStream(context.Background(), req.ResumeStreamOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleSetStreamReadonly(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.SetStreamReadonly(context.Background(), req.SetStreamReadonlyOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleJoinConsumerGroup(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	coordinator, epoch, err := s.metadata.JoinConsumerGroup(context.Background(), req.JoinConsumerGroupOp)
	if err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	} else {
		resp.JoinConsumerGroupResp = &proto.PropagatedResponse_JoinConsumerGroupResponse{
			Coordinator: coordinator,
			Epoch:       epoch,
		}
	}
	return resp
}

func (s *Server) handleLeaveConsumerGroup(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.LeaveConsumerGroup(context.Background(), req.LeaveConsumerGroupOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}

func (s *Server) handleReportConsumerGroupCoordinator(req *proto.PropagatedRequest) *proto.PropagatedResponse {
	resp := &proto.PropagatedResponse{
		Op: req.Op,
	}
	if err := s.metadata.ReportGroupCoordinator(context.Background(), req.ReportConsumerGroupCoordinatorOp); err != nil {
		resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
	}
	return resp
}
