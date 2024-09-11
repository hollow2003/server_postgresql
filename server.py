from flask import Flask, request, jsonify
import socket
import threading
import requests
import time
#import cv2
import jsonschema
from sqlalchemy import Column, Float, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import os
import shutil
import signal
import subprocess
from collections import defaultdict
engine = create_engine('postgresql://postgres:123456@192.168.1.206:6543/postgres', echo=True, future=True)
app = Flask(__name__)
Base = declarative_base()
lock = threading.Lock()
raw_cache_lock = threading.Lock()
deal_cache_lock = threading.Lock()
hostlist_lock = threading.Lock()
Session = sessionmaker(bind=engine, future=True)
session = Session()
# 存储接收到的 UDP 消息
udp_message = None
hostname_schemas_orms = {}
local_data = threading.local()
raw_data_cache = []
deal_data_cache = []
suffix = {}
timer = 1


def receive_udp_message():
    global udp_message
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
    udp_socket.bind(('', 8888))
    while True:
        data, addr = udp_socket.recvfrom(1024)
        udp_message = data.decode()
        parts = udp_message.split('+')
        ip, port = addr
        port = "8080"##modify in apply
        hostname = parts[0]
        if port and hostname:
            address = f"http://{ip}:{port}"
            hostlist_lock.acquire()
            if hostname not in hostname_schemas_orms:
                hostname_schemas_orms[hostname] = {}
            get_schema(address, hostname)
            hostlist_lock.release()
        else:
            print("无法解析消息中的端口号和主机名")
            # print(f"Received UDP message: {udp_message}")


def get_schema(address, hostname):
    global hostname_schemas_orms
    global suffix
    url = f"{address}/data_discovery"
    response = requests.get(url)
    if response.status_code == 200 and response.json():
        lock.acquire()
        schemas = response.json()
        for schema in schemas:
            exist = False
            if 'schemas' in hostname_schemas_orms[hostname]:
                for item in hostname_schemas_orms[hostname]["schemas"]:
                    if item["API"]["address"] == schema["API"]["address"]:
                        schema["API"]["proxy"] = item["API"]["proxy"];
                        schema["API"]["interested"] = item["API"]["interested"]
                        schema["API"]["cycle"] = item["API"]["cycle"]
                        exist = True
                        print("already existed")
                        break
            if not exist:
                schema["API"]["proxy"] = 0
                schema["API"]["interested"] = 0
                schema["API"]["cycle"] = 0
            schema["established"] = 0
        hostname_schemas_orms[hostname]["schemas"] = schemas
        lock.release()
        for schema in schemas:
            lock.acquire()
            # print(type(schema["id"]))
            suffix[schema["kind"] + "_" + str(schema["id"])] = 1
            schema["orm"] = generate_orm(schema_to_tree(schema['schema']), schema["kind"] + "_" + str(schema["id"]), Path=hostname, required=extract_required(schema['schema']))
            Base.metadata.create_all(engine)
            Base.metadata.clear()
            lock.release()
            if schema["orm"] is not None:
                schema["established"] = 1
            if schema.get('API').get('protocol') == 'RTSP':
                schema["c_flag"] = 0
    else:
        if hostname in hostname_schemas_orms:
            del hostname_schemas_orms[hostname]
        print(f"Failed to get {hostname} schema.")


def generate_orm(json_tree, host_name, parent_name=None, Path='', required=[]):
    require = required
    orm_classes = {}
    table = {}
    if type(json_tree) is list:
        json_tree = json_tree[0]
        if parent_name is None:
            Path = '\\' + host_name
    for key, _ in json_tree.items():
        if type(json_tree[key]) is str:
            table[key] = json_tree[key]
        elif type(json_tree[key]) is dict:
            new_path = Path + "\\" + key
            sub_orm_classes = generate_orm(json_tree[key], host_name, parent_name=key, Path=new_path, required=require)
            orm_classes.update(sub_orm_classes)
        elif type(json_tree[key]) is list:
            new_path = Path + "\\" + key
            sub_orm_classes = generate_orm(json_tree[key][0], host_name, parent_name=key, Path=new_path, required=require)
            orm_classes.update(sub_orm_classes)
    if parent_name is not None and table != {}:
        tmp = Path
        orm_classes[parent_name] = create_orm_class(Path, host_name, table, parent_name=tmp, required_fields=require)
    elif parent_name is None and table != {}:
        orm_classes['root'] = create_orm_class('\\' + host_name, host_name, table, required_fields=require)
    return orm_classes


def create_orm_class(table_name, host_name, properties, parent_name=None, required_fields=[]):
    global suffix
    class_name = f"DynamicORM_{host_name}_{suffix[host_name]}"
    suffix[host_name] += 1
    true_table_name = table_name
    if parent_name is not None:
        last_backslash_index = parent_name.rfind("\\")
        if last_backslash_index != -1:
            new_path = parent_name[:last_backslash_index]
        else:
            new_path = parent_name
        # print(new_path)
    else:
        new_path = ''
    attributes = {
        '__tablename__': true_table_name,
        '__classname__': class_name,
        'class_name_suffix': suffix[host_name] - 1,
    }
    if new_path != '' and new_path != host_name:
        attributes['timestamp'] = Column(String, ForeignKey(f"{new_path}.timestamp"), unique=True, nullable=False) if parent_name else None
        attributes['id'] = Column(Integer, primary_key=True, autoincrement=True)
    else:
        attributes['timestamp'] = Column(String, primary_key=True, unique=True, nullable=False)
    for key, value in properties.items():
        field_type = value
        is_required = key in required_fields
        if key == "timestamp":
            continue
        if field_type == 'string':
            attributes[key] = Column(String, nullable=not is_required)
        elif field_type == 'integer':
            attributes[key] = Column(Integer, nullable=not is_required)
        elif field_type == 'number':
            attributes[key] = Column(Float, nullable=not is_required)
    ORM = type(class_name, (Base,), attributes)
    return ORM


def extract_required(schema):
    required_fields = []
    if 'required' in schema:
        required_fields.extend(schema['required'])
    if 'properties' in schema:
        for prop_schema in schema['properties'].values():
            required_fields.extend(extract_required(prop_schema))
    if 'items' in schema:
        if isinstance(schema['items'], dict):
            required_fields.extend(extract_required(schema['items']))
        elif isinstance(schema['items'], list):
            for item_schema in schema['items']:
                required_fields.extend(extract_required(item_schema))
    return required_fields


def schema_to_tree(schema):
    if isinstance(schema, dict):
        if 'properties' in schema and isinstance(schema['properties'], dict):
            properties_tree = {}
            for prop_name, prop_schema in schema['properties'].items():
                properties_tree[prop_name] = schema_to_tree(prop_schema)
            return properties_tree
        elif 'items' in schema:
            if isinstance(schema['items'], dict):
                if schema['type'] == 'array':
                    items_tree = [schema_to_tree(schema['items'])]
                    return items_tree
                else:
                    return schema_to_tree(schema['items'])
            elif isinstance(schema['items'], list):
                items_tree = []
                for item_schema in schema['items']:
                    items_tree.append(schema_to_tree(item_schema))
                return items_tree
        elif 'type' in schema:
            return schema['type']
    return None


def rtsp_thread(address):
    cap = cv2.VideoCapture(address)
    while True:
        ret, frame = cap.read()
        if ret:
            cv2.imshow('RTSP Video', frame)
            cv2.waitKey(1)
        else:
            print("Failed to read frame from RTSP stream.")
            break
    cap.release()
    cv2.destroyAllWindows()


def validate_data(schema, data):
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.ValidationError as e:
        print(f"Validation error:{e}")
        return False
    return True


def extract_and_remove_sub_dicts(dictionary, sister_dic):
    keys_to_delete = []
    if type(dictionary) is dict:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                sister_dic[key] = value
                keys_to_delete.append(key)
                extract_and_remove_sub_dicts(value, sister_dic)
            if isinstance(value, list):
                sister_dic[key] = value
                keys_to_delete.append(key)
                extract_and_remove_sub_dicts(value, sister_dic)
        for key in keys_to_delete:
            del dictionary[key]
    if type(dictionary) is list:
        for item in dictionary:
            extract_and_remove_sub_dicts(item, sister_dic)
    return dictionary, sister_dic


def init_local_data():
    global hostname_schemas_orms
    local_data.hostname_schemas_orms = hostname_schemas_orms.copy()


def get_local_data():
    return local_data.hostname_schemas_orms


def get_data_periodically():
    start_time = time.time()
    init_local_data()
    global timer
    local_hostname_schemas_orms = get_local_data()
    print(local_hostname_schemas_orms)
    for key in local_hostname_schemas_orms:
        if "schemas" not in local_hostname_schemas_orms[key]:  # 如果不存在 schemas，则跳过该 hostname
            continue
        for schema in local_hostname_schemas_orms[key]["schemas"]:
            if schema["API"]["cycle"] == 0:
                continue
            if schema["API"]["interested"] != 1:
                continue
            if timer % schema["API"]["cycle"] != 0:
                print("time not satisfied")
                continue
            address = schema.get("API").get("address")   
            if (address) and (schema.get("API").get("protocol") == 'REST'):
                method = schema.get("API").get("method")
                try:
                    response = requests.request(method, address)
                    if response.status_code == 200:
                        data = response.json()
                        print(f"Received data from {key}: {data}")
                        if schema.get("established") == 1 and validate_data(schema["schema"], data):
                            if type(data) is list:
                                data = {"root": data}
                            data["orm"] = schema["orm"]
                            data["timestamp"] = str(int(time.time()))
                            print(data["timestamp"])
                            raw_cache_lock.acquire()
                            # print(str(data) + "add to raw data")
                            raw_data_cache.append(data)
                            raw_cache_lock.release()
                    else:
                        print(f"Failed to get data from {key}.")
                except requests.ConnectionError as e:
                    print(f"Connection error: {e}. Skipping {address}")
            elif (address) and (schema.get("API").get("protocol") == 'RTSP') and (schema["c_flag"] == 0):
                schema["c_flag"] = 1
                rtsp_thread_worker = threading.Thread(target=rtsp_thread, args=(address,))
                rtsp_thread_worker.start()
    end_time = time.time()
    duration = end_time - start_time
    print(f"100API took {duration} seconds to execute.")


def periodic_data_thread():
    with ThreadPoolExecutor(max_workers=16) as executor:
        while True:
            future = executor.submit(get_data_periodically)
            time.sleep(1)
            future.result()


def storage_thread():
    while True:
        loop_start_time = time.time()
        deal_cache_lock.acquire()
        if len(deal_data_cache) >= 10:
            deal_data_cache.sort(key=lambda x: x.class_name_suffix, reverse=True)
            grouped_data = defaultdict(list)
            for item in deal_data_cache:
                grouped_data[item.class_name_suffix].append(item)
                print(item.class_name_suffix)
            with Session() as session:
                try:
                    # 逐组提交数据
                    for class_name_suffix, items in grouped_data.items():
                        for item in items:
                            # print(item.class_name_suffix)
                            print(item.timestamp)
                            session.add(item)
                        session.commit()
                        print("storage success")
                    # 清空缓存
                    deal_data_cache.clear()
                except Exception as e:
                    session.rollback()
                    print(f"Error during storage: {e}")
        deal_data_cache.clear()
        deal_cache_lock.release()
        end_time = time.time()
        duration = end_time - loop_start_time
        print(f"100API took {duration} seconds to storage.")
        wait_time = max(0, 1 - duration)
        time.sleep(wait_time)


def extrace_thread():
    while True:
        loop_start_time = time.time()
        raw_cache_lock.acquire()
        if len(raw_data_cache) >= 10:
            for item in raw_data_cache:
                orm = item["orm"]
                timestamp = item["timestamp"]
                del item["timestamp"]
                del item["orm"]
                dictionary = {}
                sister_dic = {}
                dictionary, sister_dic = extract_and_remove_sub_dicts(item, sister_dic)
                # print(dictionary)
                deal_cache_lock.acquire()
                if sister_dic != {}:
                    for key, value in sister_dic.items():
                        if key in orm:
                            orm_class = orm[key]
                            if type(value) is list:
                                orm_instances = [orm_class(**item) for item in value]
                                deal_data_cache.append(orm_instances)
                            elif type(value) is dict:
                                orm_instance = orm_class(**value)
                                if timestamp is None:
                                    orm_instance.timestamp = str(time.time())
                                    timestamp = orm_instance.timestamp
                                elif timestamp is not None:
                                    orm_instance.timestamp = timestamp
                                deal_data_cache.append(orm_instance)
                                # print(timestamp)
                if dictionary != {}:
                    orm_class = orm["root"]
                    instance = orm_class()
                    for key, value in dictionary.items():
                        setattr(instance, key, value)
                        setattr(instance, "timestamp", timestamp)
                    deal_data_cache.append(instance)
                deal_cache_lock.release()
            raw_data_cache.clear()
        raw_cache_lock.release()
        end_time = time.time()
        duration = end_time - loop_start_time
        print(f"100API took {duration} seconds to extrace.")
        wait_time = max(0, 1 - duration)
        time.sleep(wait_time)


def clock_thread():
    while True:
        global timer
        timer = (timer + 1) % 10000
        time.sleep(1)


def delete_specific_files(folder_path, filenames):
    print("Deleting specified files from folder:", folder_path)
    for filename in os.listdir(folder_path):
        if filename in filenames:
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))


def delete_folder_contents(folder_path):
    print("Deleting contents of folder:", folder_path)
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def handle_exit(signum, frame):
    print("Received exit signal.")
    delete_folder_contents("/usr/local/openresty/nginx/conf/usr_conf")  # Replace with your folder path
    exit(0)


def generate_proxy_config(upstream_name, path):
    config = f"""
        location {path} {{
            proxy_pass {upstream_name};
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }}
    """
    return config


def add_config_to_nginx(config_name, config):
    with open(f'/usr/local/openresty/nginx/conf/usr_conf/{config_name}.conf', 'w') as f:
        f.write(config)


def reload_nginx():
    os.system('/usr/local/openresty/nginx/sbin/nginx -s reload')


@app.route('/add_proxy', methods=['POST'])
def add_proxy():
    data = request.json
    hostlist_lock.acquire()
    for apiitem in data:
        if apiitem["host_name"] not in hostname_schemas_orms:
            hostlist_lock.release()
            return "some device not found"
        upstream_name = apiitem["API"]["address"]
        last_part = upstream_name.split('/')[-1]
        host_name = apiitem["host_name"]
        path = f"/{host_name}/API/{last_part}"

        config = generate_proxy_config(upstream_name, path)
        add_config_to_nginx(f"{host_name}_{last_part}",config)
        reload_nginx()
        for key in hostname_schemas_orms:
            if key == host_name:
                for item in hostname_schemas_orms[key]["schemas"]:
                    if item["API"]["address"] == upstream_name:                
                        item["API"]["proxy"] = 1
    hostlist_lock.release()
    return "success"
# [{
#     host_name: "robot_1",
#     "API": {"address":"http://127.0.0.1:8089/api/move","method":"GET","protocol":"REST"}
# }]


@app.route('/delete_proxy', methods=['POST'])
def delete_proxy():
    data = request.json
    hostlist_lock.acquire()
    for apiitem in data:
        upstream_name = apiitem["API"]["address"]
        last_part = upstream_name.split('/')[-1]
        host_name = apiitem["host_name"]
        for key in hostname_schemas_orms:
            if key == host_name:
                for item in hostname_schemas_orms[key]["schemas"]:
                    if item["API"]["address"] == upstream_name:
                        item["API"]["proxy"] = 0
                        delete_specific_files('/usr/local/openresty/nginx/conf/usr_conf', [f"{host_name}_{last_part}.conf"])  
                        reload_nginx()
    hostlist_lock.release()
    return "success"
# [{
#     host_name: "robot_1",
#     "API": {"address":"http://127.0.0.1:8089/api/move"}
# }]


@app.route('/get_devices', methods=['GET'])
def get_devices():
    data = {"host": []}
    for key in hostname_schemas_orms:
        if hostname_schemas_orms[key]!={}:
            data["host"].append(key)
    print(data)
    return data


@app.route('/get_api', methods=['POST'])
def get_api():
    print(hostname_schemas_orms)
    hosts = request.json["hosts"]
    print(hosts)
    data = {}
    for key in hostname_schemas_orms:
        if key in hosts:
            data[key] = []
            for item in hostname_schemas_orms[key]["schemas"]:
                api_data={}
                api_data["id"] = item["id"]
                api_data["kind"] = item["kind"]
                api_data["address"] = item["API"]["address"]
                api_data["interested"] = item["API"]["interested"]
                api_data["method"] = item["API"]["method"]
                api_data["protocol"] = item["API"]["protocol"]
                api_data["proxy"] = item["API"]["proxy"]
                data[key].append(api_data)
    return data
# {"hosts" : ["robot_1","robot_2"]}


@app.route('/udp_message')
def get_udp_message():
    global udp_message
    return udp_message

@app.route('/add_interest_topic', methods=['POST'])
def add_interest_topic():
    # print(request.json)
    data = request.json
    hostlist_lock.acquire()
    for item in data["interest"]:
        if item["host_name"] in hostname_schemas_orms:
            for topic_and_cycle in item["interest_topic"]:
                for schema in hostname_schemas_orms[item["host_name"]]["schemas"]:
                    if topic_and_cycle["API"]["address"] == schema["API"]["address"]:
                        schema["API"]["interested"] = 1
                        schema["API"]["cycle"] = int(topic_and_cycle["API"]["cycle"])
                        break
        else:
            return "some device or topic not found"
    hostlist_lock.release()
    return "receive interest topic"
# {
#     "interest":[
#         {
#             "host_name" : "robot_1",
#             "interest_topic" :  [{"API":{"address": "http://127.0.0.1:8093/api/move",
#                                   "cycle": "1"}}]
#         }
#     ]
# }


@app.route('/cancel_interest_topic', methods=['POST'])
def cancel_interest_topic():
    data = request.json
    hostlist_lock.acquire()
    for item in data["interest"]:
        if item["host_name"] in hostname_schemas_orms:
            for topic_and_cycle in item["interest_topic"]:
                for schema in hostname_schemas_orms[item["host_name"]]["schemas"]:
                    if topic_and_cycle["API"]["address"] == schema["API"]["address"]:
                        schema["API"]["interested"] = 0
                        schema["API"]["cycle"] = 1
                        break
        else:
            print("not found")
            return "some device or topic not found"
    hostlist_lock.release()
    return "cancel interest topic"
# {
#     "interest":[
#         {
#             "host_name" : "robot_1",
#             "interest_topic" :  [{"address": "http://127.0.0.1:8093/api/move"}]
#         }
#     ]
# }


@app.route('/latest_location', methods=['GET'])
def get_latest_location():
    data = request.json
    host_name = data["host_name"]
    if host_name not in hostname_schemas_orms:
        return "device not found"
    try:
        # 连接数据库
        conn = sqlite3.connect('server.db')
        cursor = conn.cursor()
        # 构建 SQL 查询语句
        sql_query = f"SELECT * FROM `{host_name}\Location` ORDER BY timestamp DESC LIMIT 1;"
        # 执行查询
        cursor.execute(sql_query)
        # 获取查询结果
        result = cursor.fetchone()
        column_names = [description[0] for description in cursor.description]
        if result:
            # 获取查询结果的列名
            column_names = [description[0] for description in cursor.description]
            # 将查询结果转换为字典
            result_dict = dict(zip(column_names, result))
            # 返回 JSON 格式的响应
            # 关闭数据库连接
            conn.close()
            print(result_dict)
            return jsonify(result_dict)
        else:
            return jsonify({"message": "No data found"}), 404
    except Exception as e:
        # 打印异常信息以便调试
        print("An error occurred:", e)
        return jsonify({"error": str(e)}), 500


@app.route('/query_by_orm', methods=['GET'])
def query_by_orm():
    users = session.query(hostname_schemas_orms["robot_1"]["schemas"][0]["orm"]["Location"]).all()
    # 构建 SQL 查询语句
    for user in users:
        print(f"timestamp: {user.timestamp}")
    return "success"


def is_nginx_running():
    try:
        subprocess.run(['pidof', 'nginx'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False


def start_nginx():
    subprocess.run(['/usr/local/openresty/nginx/sbin/nginx'])


if not is_nginx_running():
    start_nginx()
signal.signal(signal.SIGINT, handle_exit)
timer_thread = threading.Thread(target=periodic_data_thread)
timer_thread.start()
udp_thread = threading.Thread(target=receive_udp_message)
udp_thread.start()
str_thread = threading.Thread(target=storage_thread)
str_thread.start()
ext_thread = threading.Thread(target=extrace_thread)
ext_thread.start()
clock_thread = threading.Thread(target=clock_thread)
clock_thread.start()
os.system('/usr/local/openresty/nginx/sbin/nginx -s reload')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=18080)
    session.close()
# 1s 82api
