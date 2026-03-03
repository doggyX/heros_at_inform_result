import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 验证程序配置
VERIFY_CONFIG = {
    'check_interval': 5.0,  # 检查间隔（分钟）
    'task_file': 'verify_tasks.json',  # 任务列表文件
    'log_file': 'verify.log'  # 验证日志文件
}

# 本地挂载路径配置
MOUNT_CONFIG = {
    'mount_point': os.getenv('MOUNT_POINT', '/mnt/smb_share'),
    'base_path': '测试报告 20250402'
}

# SMB共享配置（用于连接测试）
SMB_CONFIG = {
    'server': os.getenv('SMB_SERVER', '172.16.60.100'),
    'share': os.getenv('SMB_SHARE', '112000 实验室'),
    'base_path': os.getenv('SMB_BASE_PATH', '测试报告 20250402'),
    'username': os.getenv('SMB_USERNAME', ''),
    'password': os.getenv('SMB_PASSWORD', '')
}

# 错误通知钉钉机器人配置
ERROR_DINGTALK_CONFIG = {
    'webhook_url': os.getenv('ERROR_DINGTALK_WEBHOOK_URL'),
    'secret': os.getenv('ERROR_DINGTALK_SECRET')
}
