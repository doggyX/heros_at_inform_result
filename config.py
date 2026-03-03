# config.py
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 钉钉机器人配置
DINGTALK_CONFIG = {
    'webhook_url': os.getenv('DINGTALK_WEBHOOK_URL'),
    'secret': os.getenv('DINGTALK_SECRET')
}

# 数据库配置
DATABASE_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}