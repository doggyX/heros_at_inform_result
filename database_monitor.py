import pymysql
from pymysql.cursors import DictCursor
import json
import time
import hashlib
import hmac
import base64
import urllib.parse
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import threading
import logging
import os
import config as cfg

# 导入验证任务管理器
from excel_verifier import TaskManager
import verify_config as vcfg

# 配置日志
# 创建根日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # 根日志设置为DEBUG，允许所有级别的日志

# 清除默认处理器
logger.handlers.clear()

# 1. 创建控制台处理器（输出所有级别信息）
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # 控制台输出INFO及以上级别
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# 2. 创建错误日志处理器（仅记录ERROR和WARNING）
error_handler = logging.FileHandler('error.log', encoding='utf-8')
error_handler.setLevel(logging.WARNING)  # 记录WARNING及以上级别
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
error_handler.setFormatter(error_formatter)
logger.addHandler(error_handler)

# 3. 创建发送日志处理器（仅记录发送消息信息）
send_handler = logging.FileHandler('send.log', encoding='utf-8')
send_handler.setLevel(logging.INFO)
send_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
send_handler.setFormatter(send_formatter)

# 创建专门的发送日志记录器
send_logger = logging.getLogger('send')
send_logger.setLevel(logging.INFO)
send_logger.propagate = False  # 不传播到根日志

# 为发送日志记录器添加处理器
send_logger.addHandler(send_handler)  # 只添加发送日志处理器

# 为发送日志记录器添加控制台输出
send_logger.addHandler(console_handler)

class DingTalkRobot:
    """钉钉机器人客户端"""
    
    def __init__(self, webhook_url: str, secret: str, name_user_id_file: str = None):
        """
        初始化钉钉机器人
        
        Args:
            webhook_url: 钉钉机器人的Webhook地址
            secret: 加签密钥
            name_user_id_file: 姓名-user_id映射文件路径
        """
        self.webhook_url = webhook_url
        self.secret = secret
        self.timestamp = None
        self.sign = None
        self.name_user_id_map = {}
        self.name_user_id_file = name_user_id_file  # 保存映射文件路径
        if name_user_id_file:
            self.load_name_user_id_map(name_user_id_file)
    
    def load_name_user_id_map(self, file_path: str):
        """
        加载姓名-user_id映射文件
        
        Args:
            file_path: 映射文件路径
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.name_user_id_map = json.load(f)
            logger.info(f"成功加载姓名-user_id映射文件，共 {len(self.name_user_id_map)} 条记录")
        except Exception as e:
            logger.error(f"加载姓名-user_id映射文件失败: {str(e)}")
        
    def _generate_signature(self) -> Tuple[str, str]:
        """生成加签签名和时间戳"""
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = f'{timestamp}\n{self.secret}'
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, 
                            digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign
    
    def send_message(self, content: str = "", at_users: list = None, at_all: bool = False, msgtype: str = "markdown", title: str = "SI头测测试结果通知") -> bool:
        """
        发送消息到钉钉群
        
        Args:
            content: 消息内容
            at_users: 要@的用户列表（姓名列表）
            at_all: 是否@所有人
            msgtype: 消息类型，支持"text"或"markdown"
            title: 消息标题，仅用于markdown类型
            
        Returns:
            发送是否成功
        """
        try:
            # 生成签名
            self.timestamp, self.sign = self._generate_signature()
            
            # 构建完整的webhook URL
            url = f"{self.webhook_url}&timestamp={self.timestamp}&sign={self.sign}"
            
            # 处理艾特用户
            at_user_ids = []
            at_text = ""
            
            if at_users:
                for user in at_users:
                    # 从映射中获取user_id
                    user_id = self.name_user_id_map.get(user, "")
                    if user_id:
                        at_user_ids.append(user_id)
                        at_text += f"@{user_id} "
                    else:
                        logger.warning(f"姓名 {user} 没有对应的user_id，无法@")
            
            # 根据消息类型构建不同的消息结构
            if msgtype == "text":
                # text类型消息，只@创建人员，不添加额外的atUserIds
                message = {
                    "msgtype": "text",
                    "text": {
                        "content": at_text
                    },
                    "at": {
                        "atUserIds": at_user_ids,
                        "isAtAll": at_all
                    }
                }
            else:  # markdown类型
                # 保留原有逻辑，添加固定的atUserIds
                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": title,
                        "text": f"### {at_text}{content}"
                    },
                    "at": {
                        "atUserIds": at_user_ids,
                        "isAtAll": at_all
                    }
                }

            
            # 发送请求
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, data=json.dumps(message), headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    send_logger.info(f"消息发送成功: {message}")
                    return True
                else:
                    send_logger.error(f"消息发送失败: {result}")
                    logger.error(f"消息发送失败: {result}")
                    return False
            else:
                error_msg = f"HTTP请求失败: {response.status_code}"
                send_logger.error(error_msg)
                logger.error(error_msg)
                return False
                
        except Exception as e:
            error_msg = f"发送消息时发生错误: {str(e)}"
            send_logger.error(error_msg)
            logger.error(error_msg)
            return False

class DatabaseMonitor:
    """数据库监控类"""
    
    def __init__(self, db_config: Dict, robot: DingTalkRobot, record_file: str = 'monitor_records.json', load_existing: bool = False):
        """
        初始化数据库监控
        
        Args:
            db_config: 数据库配置
            robot: 钉钉机器人实例
            record_file: 记录文件路径
            load_existing: 是否加载现有记录文件
        """
        self.db_config = db_config
        self.robot = robot
        self.previous_data = {}  # 存储上一次查询的结果
        self.current_data = {}   # 存储当前查询的结果
        self.record_key_field = "唯一标识"  # 使用唯一标识作为唯一标识
        self.is_paused = False   # 监控状态：是否暂停
        self.interval_minutes = 0.5  # 监控间隔（分钟）
        self.command_file = 'control_command.json'  # 控制命令文件
        self.status_file = 'monitor_status.json'  # 状态文件
        self.record_file = record_file  # 记录文件路径
        self.last_command_time = 0  # 上次处理命令的时间
        
        # 初始化任务管理器
        self.task_manager = TaskManager(vcfg.VERIFY_CONFIG['task_file'])
        
        # 初始化状态文件
        self.update_status('running', '监控程序启动')
        
        # 处理记录文件
        if load_existing and os.path.exists(self.record_file):
            # 加载现有记录
            self.previous_data = self.load_records()
            logger.info(f"已加载 {len(self.previous_data)} 条记录")
        else:
            # 清空文件或创建新文件
            self.save_records({})
            logger.info(f"已初始化记录文件: {self.record_file}")
        
    def connect_database(self):
        """
        连接到数据库
        """
        try:
            conn = pymysql.connect(
                host=self.db_config['server'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8'
            )
            return conn
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return None
    
    def _convert_datetime_to_string(self, obj):
        """
        递归转换对象中的datetime类型为字符串
        
        Args:
            obj: 要转换的对象
            
        Returns:
            转换后的对象
        """
        if isinstance(obj, dict):
            return {key: self._convert_datetime_to_string(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_datetime_to_string(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        else:
            # 其他类型转换为字符串
            return str(obj)
    
    def save_records(self, data: Dict[str, Dict]):
        """
        保存记录到文件
        
        Args:
            data: 要保存的记录数据
        """
        try:
            # 转换数据中的datetime类型为字符串
            processed_data = self._convert_datetime_to_string(data)
            
            with open(self.record_file, 'w', encoding='utf-8') as f:
                json.dump(processed_data, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(data)} 条记录到文件: {self.record_file}")
        except Exception as e:
            logger.error(f"保存记录到文件失败: {str(e)}")
    
    def load_records(self) -> Dict[str, Dict]:
        """
        从文件加载记录
        
        Returns:
            加载的记录数据
        """
        try:
            with open(self.record_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"从文件 {self.record_file} 加载了 {len(data)} 条记录")
            return data
        except Exception as e:
            logger.error(f"从文件加载记录失败: {str(e)}")
            return {}
    
    def execute_stored_procedure(self) -> List[Dict]:
        """
        执行存储过程 sp_12_18_inform_electrical_result
        
        Returns:
            查询结果列表
        """
        conn = self.connect_database()
        if not conn:
            return []
            
        try:
            cursor = conn.cursor(DictCursor)
            
            # 执行存储过程
            cursor.callproc('sp_12_18_inform_electrical_result')
            
            # 获取结果
            results = []
            for row in cursor:
                results.append(row)
            
            cursor.close()
            conn.close()
            
            logger.info(f"查询到 {len(results)} 条记录")
            return results
            
        except Exception as e:
            logger.error(f"执行存储过程失败: {str(e)}")
            if conn:
                conn.close()
            return []
    
    def format_record_to_string(self, record: Dict) -> str:
        """
        将记录格式化为字符串用于比较
        
        Args:
            record: 单条记录
            
        Returns:
            格式化后的字符串
        """
        # 根据实际情况调整需要比较的字段
        key_parts = []
        for key, value in record.items():
            if key != self.record_key_field:  # 排除ID字段
                key_parts.append(f"{key}:{str(value)}")
        return "||".join(sorted(key_parts))
    
    def analyze_changes(self, old_data: Dict, new_data: Dict) -> Dict:
        """
        分析数据变化
        
        Args:
            old_data: 旧数据 {record_key: record}
            new_data: 新数据 {record_key: record}
            
        Returns:
            变化分析结果
        """
        changes = {
            "new_records": [],      # 新增记录
            "updated_records": [],  # 更新记录
            "result_changes": []    # 检验结果变化记录
        }
        
        all_keys = set(old_data.keys()) | set(new_data.keys())
        
        for key in all_keys:
            old_record = old_data.get(key)
            new_record = new_data.get(key)
            
            # 新增记录
            if key not in old_data:
                if new_record:
                    changes["new_records"].append(new_record)
                    # 检查新增记录是否有结果
                    result = new_record.get('组电气检验结果', '')
                    if result in ['pass', 'fail']:
                        changes["result_changes"].append({
                            'type': 'new_result',
                            'record': new_record,
                            'old_result': None,
                            'new_result': result
                        })
                continue
                
            # 删除记录（暂不处理）
            if key not in new_data:
                continue
                
            # 更新记录
            old_str = self.format_record_to_string(old_record)
            new_str = self.format_record_to_string(new_record)
            
            if old_str != new_str:
                changes["updated_records"].append(new_record)
                
                # 检查结果字段变化
                old_result = old_record.get('组电气检验结果', '')
                new_result = new_record.get('组电气检验结果', '')
                
                if old_result != new_result:
                    # 情况1: 从空变成pass或fail
                    if (old_result in ['', None]) and (new_result in ['pass', 'fail']):
                        changes["result_changes"].append({
                            'type': 'first_result',
                            'record': new_record,
                            'old_result': old_result,
                            'new_result': new_result
                        })
                    # 情况2: 从pass或fail变成其他值
                    elif (old_result in ['pass', 'fail']) and (new_result not in ['pass', 'fail']):
                        changes["result_changes"].append({
                            'type': 'result_changed',
                            'record': new_record,
                            'old_result': old_result,
                            'new_result': new_result
                        })
                    # 情况3: 在pass/fail之间变化
                    elif (old_result in ['pass', 'fail']) and (new_result in ['pass', 'fail']):
                        changes["result_changes"].append({
                            'type': 'result_updated',
                            'record': new_record,
                            'old_result': old_result,
                            'new_result': new_result
                        })
        
        return changes
    
    def generate_notification_message(self, change_info: Dict) -> str:
        """
        生成通知消息
        
        Args:
            change_info: 变化信息
            
        Returns:
            格式化后的消息
        """
        result_changes = change_info.get('result_changes', [])
        if not result_changes:
            return ""
        
        messages = []
        
        for change in result_changes:
            record = change['record']
            change_type = change['type']
            old_result = change['old_result'] or "空"
            new_result = change['new_result']
            
            # 消息类型分类：1. 出结果；2. 结果修改
            if change_type in ['new_result', 'first_result']:
                msg_type = ""
            else:
                msg_type = "结果修改，请注意: "
            
            # 获取标题所需字段
            machine_no = record.get('机台号', '未知')
            remark = record.get('备注', '')
            if not remark:
                remark = ''
            electrical_result = new_result
            
            # 构建消息标题（操作员，机台号，电气结果）
            title = f"{machine_no} {msg_type}{electrical_result} {remark}"
            
            # 构建消息内容
            content = []
            content.append(f"\n**结果变化**: {old_result} → {new_result}")
            
            # 添加详细字段信息
            content.append("\n")
            fields_to_include = [
                ('送检时间', '送检时间'),
                ('生产批号', '生产批号'),
                ('IPQC单号', 'IPQC单号'),
                ('产品编号', '产品编号')
            ]
            
            for display_name, field_name in fields_to_include:
                if field_name in record and record[field_name]:
                    content.append(f"**{display_name}**: {record[field_name]}\n")
            
            # 添加时间戳
            content.append(f"\n*通知时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
            
            # 组装完整消息  
            message = f"{title}\n" + "\n".join(content)
            messages.append(message)
        
        # 返回多条消息，每条记录单独发送
        return "\n---\n".join(messages)
    
    def update_status(self, status: str, message: str):
        """
        更新监控程序状态文件
        
        Args:
            status: 状态类型（running, paused, error, stopped）
            message: 状态描述
        """
        try:
            status_data = {
                'status': status,
                'message': message,
                'timestamp': time.time(),
                'interval_minutes': self.interval_minutes,
                'previous_data_count': len(self.previous_data),
                'is_paused': self.is_paused
            }
            
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"更新状态文件失败: {str(e)}")
    
    def check_command(self):
        """
        检查并处理控制命令
        """
        try:
            if os.path.exists(self.command_file):
                # 获取文件修改时间
                file_mtime = os.path.getmtime(self.command_file)
                
                # 如果文件没有更新，跳过处理
                if file_mtime <= self.last_command_time:
                    return
                
                # 读取并解析命令
                with open(self.command_file, 'r', encoding='utf-8') as f:
                    command_data = json.load(f)
                
                # 更新上次处理命令时间
                self.last_command_time = file_mtime
                
                # 处理命令
                command = command_data.get('command')
                params = command_data.get('params', {})
                
                logger.info(f"收到命令: {command}，参数: {params}")
                
                if command == 'pause':
                    # 暂停监控
                    self.is_paused = True
                    self.update_status('paused', '监控程序已暂停')
                    logger.info("监控程序已暂停")
                elif command == 'resume':
                    # 继续监控
                    self.is_paused = False
                    self.update_status('running', '监控程序已继续')
                    logger.info("监控程序已继续")
                elif command == 'reload_map':
                    # 重新加载姓名-user_id映射文件
                    self.robot.load_name_user_id_map('name_ding_id_map.json')
                elif command == 'change_interval':
                    # 修改监控间隔
                    new_interval = params.get('interval')
                    if new_interval and isinstance(new_interval, (int, float)) and new_interval > 0:
                        self.interval_minutes = new_interval
                        self.update_status('running', f'监控间隔已修改为 {new_interval} 分钟')
                        logger.info(f"监控间隔已修改为 {new_interval} 分钟")
                    else:
                        logger.error("无效的监控间隔参数")
                elif command == 'test_message':
                    logger.info("test_message")
                
                # 删除已处理的命令文件
                os.remove(self.command_file)
        except Exception as e:
            logger.error(f"处理命令失败: {str(e)}")
    
    def monitor_loop(self, interval_minutes: int = 2):
        """
        监控循环
        
        Args:
            interval_minutes: 监控间隔（分钟）
        """
        self.interval_minutes = interval_minutes
        logger.info(f"开始监控数据库，间隔 {interval_minutes} 分钟")
        self.update_status('running', f'监控程序启动，间隔 {interval_minutes} 分钟')
        
        while True:
            try:
                # 检查控制命令
                self.check_command()
                
                # 如果暂停，跳过查询
                if self.is_paused:
                    logger.info("监控程序已暂停，等待命令...")
                    time.sleep(5)  # 每5秒检查一次命令
                    continue
                
                # 查询数据
                logger.info("开始查询数据库...")
                results = self.execute_stored_procedure()
                
                if results:
                    # 将数据转换为字典格式，以record_key_field为键
                    self.current_data = {}
                    for record in results:
                        key = record.get(self.record_key_field)
                        if key:
                            self.current_data[str(key)] = record
                    
                    # 如果current_data为空，说明record_key_field可能不正确
                    if not self.current_data and results:
                        # 自动获取第一条记录的第一个字段作为record_key_field
                        first_record = results[0]
                        first_field = list(first_record.keys())[0]
                        self.record_key_field = first_field
                        logger.info(f"未找到指定的记录键字段，自动使用第一个字段 '{first_field}' 作为记录键")
                        # 重新构建current_data
                        self.current_data = {}
                        for record in results:
                            key = record.get(self.record_key_field)
                            if key:
                                self.current_data[str(key)] = record
                    
                    logger.info(f"使用字段 '{self.record_key_field}' 作为记录键，共构建 {len(self.current_data)} 条记录")
                    
                    # 首次运行，只存储数据不发送通知
                    if not self.previous_data:
                        logger.info("首次查询完成，初始化数据")
                        self.previous_data = self.current_data.copy()
                        # 保存首次数据到文件
                        self.save_records(self.previous_data)
                    else:
                        # 分析变化
                        changes = self.analyze_changes(self.previous_data, self.current_data)
                        
                        # 如果有结果变化，发送通知
                        if changes['result_changes']:
                            sent_count = 0
                            # 每条记录单独发送一条消息
                            for change in changes['result_changes']:
                                # 为每条记录单独构建变化信息
                                single_change_info = {
                                    'result_changes': [change]
                                }
                                # 生成单条消息
                                message = self.generate_notification_message(single_change_info)
                                if message:
                                    # 使用记录的实际信息作为标题
                                    record = change['record']
                                    
                                    # 获取创建人员，用于@用户
                                    creator = record.get('创建人员', '')
                                    
                                    # 发送第一条消息：markdown类型，
                                    success = self.robot.send_message(content=message, at_users=[creator] if creator else None)
                                    if success:
                                        sent_count += 1
                                        logger.info(f"已发送 {sent_count}/{len(changes['result_changes'])} 条变化通知")
                                        
                                        # 添加验证任务
                                        new_result = change['new_result']
                                        task_data = {
                                            'product_code': record.get('产品编号', ''),
                                            'batch_no': record.get('生产批号', ''),
                                            'ipqc_no': record.get('IPQC单号', ''),
                                            'result': new_result
                                        }
                                        task_id = self.task_manager.add_verify_task(task_data)
                                        logger.info(f"已添加验证任务: {task_id} 对应产品: {task_data['product_code']}, 批号: {task_data['batch_no']}, IPQC: {task_data['ipqc_no']}")
                                    # 发送第二条消息：text类型，只用@创建人员
                                    self.robot.send_message(at_users=[creator] if creator else None, msgtype="text")
                                    # time.sleep(3)
                                    # self.robot.send_message(at_users=['谷颖','刘鑫'], msgtype="text")
                            logger.info(f"总共发送 {sent_count} 条变化通知")
                        else:
                            logger.info("与时间间隔前无变化")
                        
                        # 更新previous_data
                        self.previous_data = self.current_data.copy()
                        # 保存更新后的数据到文件
                        self.save_records(self.previous_data)
                
                # 等待下一次查询，期间定期检查命令
                wait_time = self.interval_minutes * 60
                elapsed = 0
                while elapsed < wait_time and not self.is_paused:
                    time.sleep(5)  # 每5秒检查一次命令
                    elapsed += 5
                    self.check_command()
                
                logger.info(f"等待 {self.interval_minutes} 分钟...")
                
            except KeyboardInterrupt:
                logger.info("监控程序被用户中断")
                self.update_status('stopped', '监控程序被用户中断')
                break
            except Exception as e:
                logger.error(f"监控循环发生错误: {str(e)}")
                self.update_status('error', f"监控循环发生错误: {str(e)}")
                time.sleep(60)  # 出错后等待1分钟再重试

def main():
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='数据库监控程序')
    parser.add_argument('--load', action='store_true', help='加载现有记录文件作为初始数据，否则清空文件首次运行')
    parser.add_argument('--record-file', type=str, default='monitor_records.json', help='指定记录文件路径')
    parser.add_argument('--interval', type=float, default=1, help='监控间隔（分钟）')
    args = parser.parse_args()
    
    # 创建钉钉机器人实例
    robot = DingTalkRobot(
        webhook_url=cfg.DINGTALK_CONFIG['webhook_url'],
        secret=cfg.DINGTALK_CONFIG['secret'],
        name_user_id_file='name_ding_id_map.json'  # 姓名-user_id映射文件
    )
    
    # 创建数据库监控实例
    monitor = DatabaseMonitor(
        db_config=cfg.DATABASE_CONFIG,
        robot=robot,
        record_file=args.record_file,
        load_existing=args.load
    )
    
    # 启动监控
    monitor.monitor_loop(interval_minutes=args.interval)

if __name__ == "__main__":
    main()