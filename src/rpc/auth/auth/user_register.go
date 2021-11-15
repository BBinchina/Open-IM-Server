package rpcAuth

import (
	"Open_IM/src/common/db/mysql_model/im_mysql_model"
	"Open_IM/src/common/log"
	pbAuth "Open_IM/src/proto/auth"
	"context"
)
// 在rpc处理时， rpcservice会根据请求转发到该接口，所以在写proto的时候需要确认package
func (rpc *rpcAuth) UserRegister(_ context.Context, pb *pbAuth.UserRegisterReq) (*pbAuth.UserRegisterResp, error) {
	log.Info("", "", "rpc user_register start, [data: %s]", pb.String())

	// 调用mysql，import目录时回到底下查找api
	// 创建用户
	if err := im_mysql_model.UserRegister(pb); err != nil {
		log.Error("", "", "rpc user_register error, [data: %s] [err: %s]", pb.String(), err.Error())
		return &pbAuth.UserRegisterResp{Success: false}, err
	}
	log.Info("", "", "rpc user_register success return")

	return &pbAuth.UserRegisterResp{Success: true}, nil
}
