import hashlib
import inspect
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, List, Optional, Tuple
import urllib.parse

try:
    import requests
    from tqdm import tqdm
    import colorama
    colorama.init(autoreset=True)  # 启用 Windows ANSI 支持
except ImportError:
    print("请安装依赖库: pip install requests tqdm colorama")
    exit(1)

current_frame = inspect.currentframe()
if current_frame is None:
    current_file = __file__
else:
    current_file = inspect.getfile(current_frame)
root = os.path.dirname(os.path.abspath(current_file))

PREFIX = "https://dos-bin.zczc.cz/"
DESTINATION = os.path.join(root, 'bin')
BUF_SIZE = 65536
THREAD_SIZE = 2
TIMEOUT = 30  # 请求超时时间（秒）
MAX_RETRIES = 3  # 最大重试次数
RETRY_DELAY = 2  # 重试延迟（秒）

# 启用 ANSI 支持以正确显示进度条
os.environ['FORCE_COLOR'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'  # 确保 Python 输出不缓冲

# 下载统计信息
download_stats = {
    'success': 0,
    'failed': 0,
    'skipped': 0,
    'total_size': 0,
    'start_time': None,
    'end_time': None,
    'position': 0,
}

# 读取游戏信息
try:
    with open(os.path.join(root, 'games.json'), encoding='utf8') as f:
        game_infos = json.load(f)
except FileNotFoundError:
    print(f"错误: 未找到游戏信息文件 {os.path.join(root, 'games.json')}")
    exit(1)
except json.JSONDecodeError as e:
    print(f"错误: 游戏信息文件格式不正确 - {e}")
    exit(1)


def generate_sha256(file_path: str) -> Optional[str]:
    """计算文件的 SHA256 校验和"""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
        return sha256.hexdigest()
    except (IOError, OSError) as e:
        print(f"警告: 无法读取文件 {file_path} - {e}")
        return None


def download_with_retry(identifier: str, url: str, file_path: str,
                        retry_count: int = MAX_RETRIES, position: int = 0) -> Tuple[bool, Optional[str]]:
    """
    带重试机制和断点续传的下载函数

    Args:
        identifier: 游戏标识符
        url: 下载地址
        file_path: 保存路径
        retry_count: 重试次数
        position: 进度条位置

    Returns:
        (成功状态, 错误信息)
    """
    for attempt in range(retry_count + 1):
        try:
            # 获取文件总大小
            response = requests.head(url, timeout=TIMEOUT)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
            else:
                total_size = 0

            # 检查临时文件是否存在（断点续传）
            temp_file = file_path + '.tmp'
            downloaded_size = 0
            mode = 'wb'  # 默认写入模式

            if os.path.exists(temp_file):
                downloaded_size = os.path.getsize(temp_file)
                # 如果已下载大小等于总大小，直接重命名
                if downloaded_size == total_size and total_size > 0:
                    os.replace(temp_file, file_path)
                    # 更新统计信息
                    download_stats['success'] += 1
                    download_stats['total_size'] += downloaded_size
                    return True, None
                # 使用追加模式
                mode = 'ab'

            # 如果有已下载的部分，使用 Range 请求
            headers = {}
            if downloaded_size > 0:
                headers = {'Range': f'bytes={downloaded_size}-'}

            # 下载文件
            response = requests.get(url, headers=headers, stream=True, timeout=TIMEOUT)
            response.raise_for_status()

            # 如果服务器不支持 Range 请求，从头开始
            if response.status_code == 416:  # Range Not Satisfiable
                downloaded_size = 0
                mode = 'wb'
                response = requests.get(url, stream=True, timeout=TIMEOUT)
                response.raise_for_status()

            # 打开文件并下载
            with open(temp_file, mode) as f, tqdm(
                desc=f"{identifier}",
                total=total_size,
                initial=downloaded_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                position=position,
                leave=True,
                ncols=80,
                disable=False,
                ascii=None  # 使用 Unicode 字符
            ) as pbar:
                for chunk in response.iter_content(chunk_size=BUF_SIZE):
                    # 检查是否收到中断信号
                    if download_stats.get('interrupt', False):
                        response.close()
                        pbar.close()
                        return False, "用户中断下载"

                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        pbar.update(len(chunk))
                pbar.close()

            # 重命名为正式文件
            os.replace(temp_file, file_path)

            # 更新统计信息
            download_stats['success'] += 1
            download_stats['total_size'] += downloaded_size

            return True, None

        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if attempt < retry_count:
                print(f"下载失败 ({attempt + 1}/{retry_count}): {identifier} - {error_msg}")
                print(f"等待 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)
            else:
                return False, error_msg

        except Exception as e:
            error_msg = f"未知错误: {str(e)}"
            if attempt < retry_count:
                print(f"下载失败 ({attempt + 1}/{retry_count}): {identifier} - {error_msg}")
                print(f"等待 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)
            else:
                return False, error_msg

    return False, "重试次数已用完"


def verify_file(file_path: str, expected_sha256: str) -> bool:
    """验证文件是否正确"""
    actual_sha256 = generate_sha256(file_path)
    if actual_sha256 is None:
        return False
    return actual_sha256 == expected_sha256


def download(identifier: str, url: str, file_path: str, position: int = 0) -> Tuple[bool, str]:
    """
    下载单个游戏文件

    Args:
        identifier: 游戏标识符
        url: 下载地址
        file_path: 保存路径
        position: 进度条位置

    Returns:
        (成功状态, 消息)
    """
    # 检查文件是否已存在且校验通过
    if os.path.isfile(file_path):
        expected_sha256 = game_infos['games'].get(identifier, {}).get('sha256')
        if expected_sha256 and verify_file(file_path, expected_sha256):
            download_stats['skipped'] += 1
            return True, f"已存在且校验通过: {identifier}"

    # 下载文件
    success, error_msg = download_with_retry(identifier, url, file_path, position)

    if success:
        # 下载后再次校验
        expected_sha256 = game_infos['games'].get(identifier, {}).get('sha256')
        if expected_sha256 and not verify_file(file_path, expected_sha256):
            download_stats['failed'] += 1
            download_stats['success'] -= 1
            # 删除校验失败的文件，下次重新下载
            try:
                os.remove(file_path)
            except:
                pass
            return False, f"校验失败: {identifier} (SHA256 不匹配)"

        return True, f"下载成功: {identifier}"
    else:
        download_stats['failed'] += 1
        return False, f"下载失败: {identifier} ({error_msg})"


def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    size = float(size_bytes)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


def print_summary(downloaded: List[str]):
    """打印下载摘要"""
    print("\n" + "=" * 60)
    print("下载完成!")
    print("=" * 60)

    elapsed_time = download_stats['end_time'] - download_stats['start_time']
    speed = download_stats['total_size'] / elapsed_time / 1024 / 1024 if elapsed_time > 0 else 0

    print(f"总游戏数: {len(game_infos['games'])}")
    print(f"下载成功: {download_stats['success']}")
    print(f"跳过 (已存在): {download_stats['skipped']}")
    print(f"失败: {download_stats['failed']}")
    print(f"下载总量: {format_size(download_stats['total_size'])}")
    print(f"平均速度: {speed:.2f} MB/s")
    print(f"耗时: {elapsed_time:.2f} 秒")

    if download_stats['failed'] > 0:
        print("\n⚠️  有游戏下载失败，建议重新运行脚本")
    else:
        print("\n✅ 所有游戏下载完成!")


def check_network_connection() -> bool:
    """检查网络连接"""
    try:
        response = requests.head(PREFIX, timeout=5)
        return response.status_code < 500
    except requests.exceptions.RequestException:
        return False


def main(prefix: str = PREFIX, destination: str = DESTINATION):
    """主函数"""
    print("中文 DOS 游戏下载器")
    print("=" * 60)

    # 检查网络连接
    if not check_network_connection():
        print("❌ 错误: 无法连接到服务器，请检查网络连接")
        return []

    # 创建目标文件夹
    os.makedirs(destination, exist_ok=True)

    # 初始化统计信息
    download_stats['start_time'] = time.time()

    downloaded = []
    failed_list = []

    # 创建任务队列
    task_queue = Queue()

    # 将所有任务放入队列
    for identifier in game_infos['games'].keys():
        file_path = os.path.normcase(os.path.join(destination, identifier + '.zip'))
        url = prefix + urllib.parse.quote(identifier) + '.zip'
        task_queue.put((identifier, url, file_path))

    total_tasks = task_queue.qsize()
    print(f"准备下载 {total_tasks} 个游戏...")

    # 使用线程池下载
    def worker():
        """工作线程函数"""
        while True:
            try:
                # 检查是否收到中断信号
                if download_stats.get('interrupt', False):
                    break

                # 获取任务，设置超时以支持中断
                try:
                    identifier, url, file_path = task_queue.get(timeout=0.1)
                except:
                    if task_queue.empty():
                        break
                    continue

                # 为每个任务分配递增的 position
                position = download_stats['position'] + 1
                download_stats['position'] = position
                success, message = download(identifier, url, file_path, position)
                if success:
                    downloaded.append(identifier)
                else:
                    failed_list.append((identifier, message))

                task_queue.task_done()

            except KeyboardInterrupt:
                download_stats['interrupt'] = True
                break
            except Exception as e:
                task_queue.task_done()
                # 不要在异常处理中访问可能未定义的变量
                pass

    # 启动工作线程
    futures = []

    try:
        with ThreadPoolExecutor(max_workers=THREAD_SIZE) as executor:
            # 启动工作线程
            for i in range(THREAD_SIZE):
                future = executor.submit(worker)
                futures.append(future)

            # 等待所有任务完成
            task_queue.join()

            # 等待所有工作线程结束
            for future in futures:
                future.result()

    except KeyboardInterrupt:
        print("\n\n⚠️  检测到 Ctrl+C，正在停止下载...")
        download_stats['interrupt'] = True
        print(f"已下载 {len(downloaded)} 个游戏")
        print(f"已跳过 {download_stats['skipped']} 个游戏")
        print(f"失败 {download_stats['failed']} 个游戏")
        print("正在等待正在下载的任务完成...")

        # 等待正在进行的任务
        try:
            for future in futures:
                future.result(timeout=2)
        except:
            pass

        print("您可以重新运行脚本继续下载")
        return downloaded


if __name__ == '__main__':
    main()
