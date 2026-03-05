# 数据库监控与Excel验证系统

## 项目简介

本系统是一个自动化的数据库监控与Excel验证系统，主要功能包括：

- 监控数据库中的电气检验结果变化
- 发送钉钉通知提醒相关人员
- 自动验证Excel文件中的检验结果
- 记录验证过程和结果

## 功能特性

### 1. 数据库监控
- 定期执行存储过程查询电气检验结果
- 分析数据变化，检测结果状态变更
- 发送钉钉通知给相关人员
- 自动添加验证任务到任务列表

### 2. Excel验证
- 根据产品编号和生产批号查找对应Excel文件
- 支持产品编号到单元格位置的映射配置
- 验证文件名中的结果关键字
- 验证指定单元格中的结果值
- 记录验证成功和失败的详细信息

### 3. 任务管理
- 自动生成符合日期格式的任务编号（YYMMDD+3位编号）
- 记录任务执行状态和结果
- 提供任务处理回调接口

### 4. 配置管理
- 支持特殊路径映射配置
- 支持产品编号到单元格位置的映射配置
- 环境变量配置钉钉机器人信息

## 项目结构

```
├── config.py              # 配置文件
├── control_monitor.py     # 监控程序控制端
├── database_monitor.py    # 数据库监控主程序
├── excel_verifier.py      # Excel验证程序
├── verify_config.py       # 验证程序配置
├── name_ding_id_map.json  # 姓名-钉钉ID映射
├── special_paths.json     # 特殊路径和单元格映射
├── monitor_records.json   # 监控记录
├── verify_tasks.json      # 验证任务列表
├── verify_success_records.json # 验证成功记录
├── error.log              # 错误日志
├── send.log               # 发送日志
├── verify.log             # 验证日志
└── .env                   # 环境变量配置
```

## 安装说明

### 1. 环境要求
- Python 3.7+
- 依赖库：
  - pymysql
  - requests
  - openpyxl
  - python-dotenv

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

创建 `.env` 文件，添加以下配置：

```
# 钉钉机器人配置
DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?access_token=your_token
DINGTALK_SECRET=your_secret

# 数据库配置
DB_SERVER=your_db_server
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=your_db_name
```

### 4. 配置特殊路径映射

编辑 `special_paths.json` 文件：

```json
{
  "product_code_to_folder": {
    "PROD001": "产品001文件夹",
    "PROD002": "产品002专用文件夹"
  },
  "product_code_to_cell": {
    "PROD001": "F11",
    "PROD002": "G12"
  }
}
```

## 使用方法

### 1. 启动数据库监控

```bash
python3 database_monitor.py --interval 1
```

参数说明：
- `--interval`：监控间隔（分钟），默认1分钟
- `--load`：加载现有记录文件作为初始数据
- `--record-file`：指定记录文件路径

### 2. 启动Excel验证程序

```bash
python3 excel_verifier.py
```

### 3. 控制监控程序

```bash
python3 control_monitor.py
```

控制命令：
- `pause`：暂停监控
- `resume`：继续监控
- `reload`：重新加载姓名-钉钉ID映射
- `interval <分钟>`：修改监控间隔
- `test [<user_id1> <user_id2>...]`：发送测试消息
- `status`：查看监控状态
- `verify-interval <分钟>`：修改核对程序检查间隔
- `clear-tasks`：清空验证任务
- `help`：显示帮助信息
- `exit`：退出控制程序

## 任务编号规则

任务编号格式：`YYMMDD+3位编号`

- `YYMMDD`：日期，从当天8点到次日8点均使用当天日期
- `3位编号`：当天任务的递增编号，从001开始

示例：260305001（2026年3月5日第1个任务）

## 验证流程

1. 数据库监控程序检测到电气检验结果变化
2. 发送钉钉通知给相关人员
3. 添加验证任务到任务列表
4. Excel验证程序定期检查任务列表
5. 根据产品编号和生产批号查找Excel文件
6. 验证文件名中的结果关键字
7. 验证指定单元格中的结果值
8. 记录验证结果到日志和成功记录文件
9. 发送验证失败通知

## 日志文件

- `error.log`：错误日志
- `send.log`：发送通知日志
- `verify.log`：验证程序日志
- `monitor_records.json`：监控记录
- `verify_success_records.json`：验证成功记录

## 注意事项

1. 确保数据库连接正常
2. 确保Excel文件路径可访问
3. 确保钉钉机器人配置正确
4. 定期清理日志文件，避免占用过多磁盘空间

## 故障排查

1. 检查数据库连接配置
2. 检查网络连接
3. 检查Excel文件路径和权限
4. 查看日志文件了解具体错误信息

## 版本历史

- v1.0：初始版本，实现基本监控和验证功能
- v1.1：添加产品编号到单元格位置的映射功能
- v1.2：优化任务编号生成逻辑，使用变量记录当天任务数
