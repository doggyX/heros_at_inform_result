#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库监控程序控制端
功能：
1. 暂停（不清除上一次记录，当发出继续命令，以上一次记录作为参照继续查询）
2. 继续
3. 重新加载姓名-钉钉号字典
4. 修改时间间隔
5. 给出发测试消息接口
"""

import os
import json
import time
import logging
import sys
import threading

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 通信文件路径
COMMAND_FILE = 'control_command.json'
STATUS_FILE = 'monitor_status.json'







class MonitorController:
    """数据库监控程序控制器"""
    
    def __init__(self):
        self.base_command_file = COMMAND_FILE
        self.base_status_file = STATUS_FILE
    
    def send_command(self, command, params=None, instance_id=None):
        """
        发送命令到监控程序
        
        Args:
            command: 命令类型
            params: 命令参数
            instance_id: 实例ID，None表示广播到所有实例
        """
        try:
            command_data = {
                'command': command,
                'params': params or {},
                'timestamp': time.time()
            }
            
            if instance_id:
                # 发送命令到特定实例
                command_file = self.base_command_file.replace('control_command', f'control_command_{instance_id}')
                with open(command_file, 'w', encoding='utf-8') as f:
                    json.dump(command_data, f, ensure_ascii=False, indent=2)
                logger.info(f"已发送命令: {command} 到实例 {instance_id}，参数: {params}")
                return True
            else:
                # 广播命令到所有实例
                # 1. 发送到默认实例
                with open(self.base_command_file, 'w', encoding='utf-8') as f:
                    json.dump(command_data, f, ensure_ascii=False, indent=2)
                logger.info(f"已发送命令: {command} 到默认实例，参数: {params}")
                
                # 2. 发送到所有命名实例（通过查找control_command_*.json文件）
                import glob
                instance_files = glob.glob('control_command_*.json')
                for file_path in instance_files:
                    if file_path != self.base_command_file:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(command_data, f, ensure_ascii=False, indent=2)
                        # 提取实例ID
                        instance_id = file_path.replace('control_command_', '').replace('.json', '')
                        logger.info(f"已发送命令: {command} 到实例 {instance_id}，参数: {params}")
                return True
        except Exception as e:
            logger.error(f"发送命令失败: {str(e)}")
            return False
    
    def get_monitor_status(self, instance_id=None):
        """
        获取监控程序状态
        
        Args:
            instance_id: 实例ID，None表示获取所有实例的状态
        
        Returns:
            监控程序状态字典或包含所有实例状态的字典
        """
        try:
            if instance_id:
                # 获取特定实例的状态
                status_file = self.base_status_file.replace('monitor_status', f'monitor_status_{instance_id}')
                if os.path.exists(status_file):
                    with open(status_file, 'r', encoding='utf-8') as f:
                        return {
                            'instance_id': instance_id,
                            'status': json.load(f)
                        }
                return {
                    'instance_id': instance_id,
                    'status': {'status': 'unknown', 'message': '状态文件不存在'}
                }
            else:
                # 获取所有实例的状态
                all_status = {}
                
                # 1. 获取默认实例状态
                if os.path.exists(self.base_status_file):
                    with open(self.base_status_file, 'r', encoding='utf-8') as f:
                        all_status['default'] = json.load(f)
                else:
                    all_status['default'] = {'status': 'unknown', 'message': '状态文件不存在'}
                
                # 2. 获取所有命名实例状态
                import glob
                status_files = glob.glob('monitor_status_*.json')
                for file_path in status_files:
                    if file_path != self.base_status_file:
                        instance_id = file_path.replace('monitor_status_', '').replace('.json', '')
                        with open(file_path, 'r', encoding='utf-8') as f:
                            all_status[instance_id] = json.load(f)
                
                return all_status
        except Exception as e:
            logger.error(f"获取监控状态失败: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def pause_monitor(self):
        """
        暂停监控程序
        """
        return self.send_command('pause')
    
    def resume_monitor(self):
        """
        继续监控程序
        """
        return self.send_command('resume')
    
    def reload_name_user_id_map(self):
        """
        重新加载姓名-user_id映射文件
        """
        return self.send_command('reload_map')
    
    def change_interval(self, interval_minutes):
        """
        修改监控时间间隔
        
        Args:
            interval_minutes: 新的时间间隔（分钟）
        """
        return self.send_command('change_interval', {'interval': interval_minutes})
    
    def send_test_message(self, at_user_ids=None):
        """
        发送测试消息
        
        Args:
            at_user_ids: 要@的用户ID列表
        """
        # 构造完整的markdown格式测试消息
        title = "测试消息"
        content = "# 测试消息\n\n这是一条测试消息，用于验证系统通知功能是否正常。\n\n**测试时间**：" + time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 处理艾特文本
        at_text = ""
        if at_user_ids:
            # 生成@文本，用于显示在消息内容中
            for user_id in at_user_ids:
                at_text += f"@{user_id} "
        
        # 构造完整的钉钉markdown消息结构
        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"{at_text}\n{content}"
            },
            "at": {
                "atUserIds": at_user_ids or [],
                "isAtAll": False
            }
        }
        
        # 发送命令到监控程序
        params = {
            'message': message,
            'at_user_ids': at_user_ids
        }
        
        return self.send_command('test_message', params)
    
    def set_verify_interval(self, interval_minutes):
        """
        修改核对程序检查间隔
        
        Args:
            interval_minutes: 新的检查间隔（分钟）
        """
        try:
            # 发送命令到验证程序
            command_file = 'control_command_verify.json'
            command_data = {
                'command': 'change_interval',
                'params': {'interval': interval_minutes},
                'timestamp': time.time()
            }
            
            with open(command_file, 'w', encoding='utf-8') as f:
                json.dump(command_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"已发送修改核对程序检查间隔命令: {interval_minutes} 分钟")
            return True
        except Exception as e:
            logger.error(f"修改核对程序检查间隔失败: {str(e)}")
            return False
    
    def clear_verify_tasks(self):
        """
        清空verify_tasks.json文件
        """
        try:
            # 发送命令到验证程序
            command_file = 'control_command_verify.json'
            command_data = {
                'command': 'clear_tasks',
                'params': {},
                'timestamp': time.time()
            }
            
            with open(command_file, 'w', encoding='utf-8') as f:
                json.dump(command_data, f, ensure_ascii=False, indent=2)
            
            logger.info("已发送清空verify_tasks.json文件命令")
            return True
        except Exception as e:
            logger.error(f"清空verify_tasks.json文件失败: {str(e)}")
            return False
    
    def show_help(self):
        """
        显示帮助信息
        """
        help_text = """
数据库监控程序控制命令
-----------------------
1. pause          - 暂停监控程序
2. resume         - 继续监控程序
3. reload         - 重新加载姓名-user_id映射文件
4. interval <分钟> - 修改监控时间间隔
5. test [<user_id1> <user_id2>...] - 发送测试消息（直接使用user_id）
6. status         - 查看监控程序状态
7. verify-interval <分钟> - 修改核对程序检查间隔
8. clear-tasks    - 清空verify_tasks.json文件
9. help           - 显示帮助信息
10. exit          - 退出控制程序
        """
        print(help_text)
    
    def run(self):
        """
        运行控制程序
        """
        logger.info("数据库监控程序控制端启动")
        

        
        self.show_help()
        
        while True:
            try:
                command = input("\n请输入命令: ").strip()
                if not command:
                    continue
                
                # 解析命令
                parts = command.split()
                cmd = parts[0].lower()
                
                if cmd == 'exit':
                    logger.info("控制程序退出")
                    break
                elif cmd == 'help':
                    self.show_help()
                elif cmd == 'pause':
                    self.pause_monitor()
                elif cmd == 'resume':
                    self.resume_monitor()
                elif cmd == 'reload':
                    self.reload_name_user_id_map()
                elif cmd == 'interval':
                    if len(parts) == 2:
                        try:
                            interval = float(parts[1])
                            self.change_interval(interval)
                        except ValueError:
                            logger.error("时间间隔必须是数字")
                    else:
                        logger.error("请输入正确的时间间隔，格式: interval <分钟>")
                elif cmd == 'test':
                    # 直接使用参数作为要@的user_id
                    at_user_ids = parts[1:]  # 直接将所有参数作为user_id
                    
                    # 可以只发送测试消息，不@任何人
                    self.send_test_message(at_user_ids)
                    logger.info(f"已发送测试消息，艾特用户ID: {at_user_ids if at_user_ids else '无'}")
                elif cmd == 'status':
                    status = self.get_monitor_status()
                    logger.info(f"监控程序状态: {json.dumps(status, ensure_ascii=False, indent=2)}")
                elif cmd == 'verify-interval':
                    if len(parts) == 2:
                        try:
                            interval = float(parts[1])
                            self.set_verify_interval(interval)
                        except ValueError:
                            logger.error("时间间隔必须是数字")
                    else:
                        logger.error("请输入正确的时间间隔，格式: verify-interval <分钟>")
                elif cmd == 'clear-tasks':
                    self.clear_verify_tasks()
                else:
                    logger.error("未知命令，请输入 help 查看可用命令")
                    
            except KeyboardInterrupt:
                logger.info("控制程序退出")
                break
            except Exception as e:
                logger.error(f"命令执行失败: {str(e)}")

if __name__ == "__main__":
    controller = MonitorController()
    controller.run()
