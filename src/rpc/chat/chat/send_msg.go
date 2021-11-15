package rpcChat

import (
	"Open_IM/src/api/group"
	"Open_IM/src/common/config"
	"Open_IM/src/common/constant"
	http2 "Open_IM/src/common/http"
	"Open_IM/src/common/log"
	"Open_IM/src/grpc-etcdv3/getcdv3"
	pbChat "Open_IM/src/proto/chat"
	pbGroup "Open_IM/src/proto/group"
	"Open_IM/src/push/content_struct"
	"Open_IM/src/utils"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type MsgCallBackReq struct {
	SendID      string `json:"sendID"`
	RecvID      string `json:"recvID"`
	Content     string `json:"content"`
	SendTime    int64  `json:"sendTime"`
	MsgFrom     int32  `json:"msgFrom"`
	ContentType int32  `json:"contentType"`
	SessionType int32  `json:"sessionType"`
	PlatformID  int32  `json:"senderPlatformID"`
}
type MsgCallBackResp struct {
	ErrCode         int32  `json:"errCode"`
	ErrMsg          string `json:"errMsg"`
	ResponseErrCode int32  `json:"responseErrCode"`
	ResponseResult  struct {
		ModifiedMsg string `json:"modifiedMsg"`
		Ext         string `json:"ext"`
	}
}
// msggateway发送过来的消息
func (rpc *rpcChat) UserSendMsg(_ context.Context, pb *pbChat.UserSendMsgReq) (*pbChat.UserSendMsgResp, error) {
	replay := pbChat.UserSendMsgResp{}
	log.InfoByKv("sendMsg", pb.OperationID, "args", pb.String())
	if !utils.VerifyToken(pb.Token, pb.SendID) {
		return returnMsg(&replay, pb, http.StatusUnauthorized, "token validate err,not authorized", "", 0)
	}
	// 根据用户id 时间戳 跟随机数 生成消息id
	serverMsgID := GetMsgID(pb.SendID)
	pbData := pbChat.WSToMsgSvrChatMsg{}
	pbData.MsgFrom = pb.MsgFrom
	pbData.SessionType = pb.SessionType
	pbData.ContentType = pb.ContentType
	pbData.Content = pb.Content
	pbData.RecvID = pb.RecvID
	pbData.ForceList = pb.ForceList
	pbData.OfflineInfo = pb.OffLineInfo
	pbData.Options = pb.Options
	pbData.PlatformID = pb.PlatformID
	pbData.ClientMsgID = pb.ClientMsgID
	pbData.SendID = pb.SendID
	pbData.SenderNickName = pb.SenderNickName
	pbData.SenderFaceURL = pb.SenderFaceURL
	pbData.MsgID = serverMsgID
	pbData.OperationID = pb.OperationID
	pbData.Token = pb.Token
	pbData.SendTime = utils.GetCurrentTimestampByNano()
	m := MsgCallBackResp{}
	// 这个消息回调是做什么， 客户端发送消息 会进行哪的跳转，做敏感词判断？
	if config.Config.MessageCallBack.CallbackSwitch {
		bMsg, err := http2.Post(config.Config.MessageCallBack.CallbackUrl, MsgCallBackReq{
			SendID:      pb.SendID,
			RecvID:      pb.RecvID,
			Content:     pb.Content,
			SendTime:    pbData.SendTime,
			MsgFrom:     pbData.MsgFrom,
			ContentType: pb.ContentType,
			SessionType: pb.SessionType,
			PlatformID:  pb.PlatformID,
		}, "application/json; charset=utf-8")
		if err != nil {
			log.ErrorByKv("callback to Business server err", pb.OperationID, "args", pb.String(), "err", err.Error())
			return returnMsg(&replay, pb, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), "", 0)
		} else if err = json.Unmarshal(bMsg, &m); err != nil {
			log.ErrorByKv("ws json Unmarshal err", pb.OperationID, "args", pb.String(), "err", err.Error())
			return returnMsg(&replay, pb, 200, err.Error(), "", 0)
		} else {
			if m.ErrCode != 0 {
				return returnMsg(&replay, pb, m.ResponseErrCode, m.ErrMsg, "", 0)
			} else {
				pbData.Content = m.ResponseResult.ModifiedMsg
			}
		}
	}
	// 根据消息类型， 个人消息通过kafka推送 群组消息 需要先获取群组成员再发往kafka
	switch pbData.SessionType {
	case constant.SingleChatType:
		// 消息还要发往发送者的kafka队列，目的是：
		err1 := rpc.sendMsgToKafka(&pbData, pbData.RecvID)
		err2 := rpc.sendMsgToKafka(&pbData, pbData.SendID)
		if err1 != nil || err2 != nil {
			return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
		}
		return returnMsg(&replay, pb, 0, "", serverMsgID, pbData.SendTime)
	case constant.GroupChatType:
		etcdConn := getcdv3.GetConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImGroupName)
		client := pbGroup.NewGroupClient(etcdConn)
		req := &pbGroup.GetGroupAllMemberReq{
			GroupID:     pbData.RecvID,
			Token:       pbData.Token,
			OperationID: pbData.OperationID,
		}
		reply, err := client.GetGroupAllMember(context.Background(), req)
		if err != nil {
			log.Error(pbData.Token, pbData.OperationID, "rpc send_msg getGroupInfo failed, err = %s", err.Error())
			return returnMsg(&replay, pb, 201, err.Error(), "", 0)
		}
		if reply.ErrorCode != 0 {
			log.Error(pbData.Token, pbData.OperationID, "rpc send_msg getGroupInfo failed, err = %s", reply.ErrorMsg)
			return returnMsg(&replay, pb, reply.ErrorCode, reply.ErrorMsg, "", 0)
		}
		var addUidList []string
		// 处理 踢成员 成员退出的消息
		switch pbData.ContentType {
		case constant.KickGroupMemberTip:
			var notification content_struct.NotificationContent
			var kickContent group.KickGroupMemberReq
			err := utils.JsonStringToStruct(pbData.Content, &notification)
			if err != nil {
				log.ErrorByKv("json unmarshall err", pbData.OperationID, "err", err.Error())
				return returnMsg(&replay, pb, 200, err.Error(), "", 0)
			} else {
				err := utils.JsonStringToStruct(notification.Detail, &kickContent)
				if err != nil {
					log.ErrorByKv("json unmarshall err", pbData.OperationID, "err", err.Error())
					return returnMsg(&replay, pb, 200, err.Error(), "", 0)
				}
				for _, v := range kickContent.UidListInfo {
					addUidList = append(addUidList, v.UserId)
				}
			}
		case constant.QuitGroupTip:
			addUidList = append(addUidList, pbData.SendID)
		default:
		}
		groupID := pbData.RecvID
		// 遍历成员列表， 往成员推送消息
		for i, v := range reply.MemberList {
			pbData.RecvID = v.UserId + " " + groupID
			err := rpc.sendMsgToKafka(&pbData, utils.IntToString(i))
			if err != nil {
				return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
			}
		}
		// 被踢出 获取 退出的用户也要收到一条消息
		for i, v := range addUidList {
			pbData.RecvID = v + " " + groupID
			err := rpc.sendMsgToKafka(&pbData, utils.IntToString(i+1))
			if err != nil {
				return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
			}
		}
		return returnMsg(&replay, pb, 0, "", serverMsgID, pbData.SendTime)
	default:

	}

	return returnMsg(&replay, pb, 203, "unkonwn sessionType", "", 0)

}
//供内部调用， 调用common的kafka包来推送，producer在启动chat服务是实例化
func (rpc *rpcChat) sendMsgToKafka(m *pbChat.WSToMsgSvrChatMsg, key string) error {
	pid, offset, err := rpc.producer.SendMessage(m, key)
	if err != nil {
		log.ErrorByKv("kafka send failed", m.OperationID, "send data", m.String(), "pid", pid, "offset", offset, "err", err.Error(), "key", key)
	}
	return err
}
func GetMsgID(sendID string) string {
	t := time.Now().Format("2006-01-02 15:04:05")
	return t + "-" + sendID + "-" + strconv.Itoa(rand.Int())
}
func returnMsg(replay *pbChat.UserSendMsgResp, pb *pbChat.UserSendMsgReq, errCode int32, errMsg, serverMsgID string, sendTime int64) (*pbChat.UserSendMsgResp, error) {
	replay.ErrCode = errCode
	replay.ErrMsg = errMsg
	replay.ReqIdentifier = pb.ReqIdentifier
	replay.ClientMsgID = pb.ClientMsgID
	replay.ServerMsgID = serverMsgID
	replay.SendTime = sendTime
	return replay, nil
}
