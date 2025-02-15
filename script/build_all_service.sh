#!/usr/bin/env bash
# 系统配置
source ./style_info.cfg
# 编译项目的路径
source ./path_info.cfg

source ./function.sh

bin_dir="../bin"
logs_dir="../logs"
sdk_db_dir="../db/sdk/"
#Automatically created when there is no bin, logs folder
if [ ! -d $bin_dir ]; then
  mkdir -p $bin_dir
fi
if [ ! -d $logs_dir ]; then
  mkdir -p $logs_dir
fi
if [ ! -d $sdk_db_dir ]; then
  mkdir -p $sdk_db_dir
fi

#begin path
begin_path=$PWD
# 遍历待编译的模块  src/api  src/rpc/user/ src/rpc/friend/ src/rpc/group/ src/rpc/auth/ src/gateway src/msg_transfer/ src/rpc/chat/  src/push/ src/Open-IM-SDK-Core/
for ((i = 0; i < ${#service_source_root[*]}; i++)); do
  cd $begin_path
  service_path=${service_source_root[$i]}
  cd $service_path && echo -e "${SKY_BLUE_PREFIX}Current directory: $PWD $COLOR_SUFFIX"
  make install && echo -e "${SKY_BLUE_PREFIX}build ${service_names[$i]} success,moving binary file to the bin directory${COLOR_SUFFIX}" &&
    echo -e "${SKY_BLUE_PREFIX}Successful moved ${service_names[$i]} to the bin directory${COLOR_SUFFIX}\n"
done
