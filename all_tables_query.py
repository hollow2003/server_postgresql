from flask import Flask, request, render_template
from dash import Dash, html, dash_table, dcc
from dash.dependencies import Input, Output, State
import pandas as pd
from sqlalchemy import create_engine, MetaData, inspect, Table
import dash

app = Flask(__name__)

# 配置数据库连接
DATABASE_URI = 'postgresql://postgres:123456@192.168.1.206:6543/postgres'
engine = create_engine(DATABASE_URI)

# 创建Dash应用并集成到Flask应用中
dash_app = Dash(__name__, server=app, url_base_pathname='/')

# 存储报警信息
alert_messages = []

# 连接数据库并获取所有表名
def get_table_names():
    return ['\\MovingRobot_215215', '\\plc_244']

# 连接数据库并获取数据
def get_data(table_name):
    with engine.connect() as connection:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select().order_by(table.c.timestamp.desc()).limit(10)
        result_proxy = connection.execute(query)
        df = pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())
        df['timestamp'] = pd.to_numeric(df['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
        df['timestamp'] = df['timestamp'].dt.strftime('%Y年%m月%d日 %H时%M分%S秒')
        if table_name == "\\plc_244":
            df = df[['timestamp', 'axis_actualPosition', 'axis_actualVelocity']]
    return df.to_dict('records')

def get_columns(table_name):
    colums = inspect(engine).get_columns(table_name)
    if table_name == "\\plc_244":
        colums = [col for col in colums if col['name'] in ['timestamp','axis_actualPosition', 'axis_actualVelocity']]
    return colums

# 初始化表数据
initial_data = {table: get_data(table) for table in get_table_names()}

# 创建一个用于展示表格的布局
def create_table_layout(table_name, data):
    return html.Div([
        html.H2(f"Table: {table_name}"),
        dash_table.DataTable(
            id={'type': 'dynamic-table', 'index': table_name},
            columns=[{'name': col["name"], 'id': col["name"]} for col in get_columns(table_name)],
            data=data
        ),
        html.Button('Update Data', id={'type': 'update-button', 'index': table_name}, n_clicks=0)
    ])

# Dash应用布局
dash_app.layout = html.Div(
    children=[
        dcc.Interval(
            id='interval-component',
            interval=5000,  # 每5秒刷新一次
            n_intervals=0
        ),
        # 添加显示报警信息的表格
        html.H2("Alert Messages"),
        dash_table.DataTable(
            id='alert-table',
            columns=[
                {'name': 'timestamp', 'id': 'timestamp'},
                {'name': 'message', 'id': 'message'},
                
            ],
            data=[]  # 初始为空，将在回调中填充数据
        )        
    ] + 
    [create_table_layout(table, data) for table, data in initial_data.items()]
)

# 回调函数用于更新表格数据
@dash_app.callback(
    Output({'type': 'dynamic-table', 'index': dash.dependencies.ALL}, 'data'),
    [Input({'type': 'update-button', 'index': dash.dependencies.ALL}, 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [State({'type': 'dynamic-table', 'index': dash.dependencies.ALL}, 'id')]
)
def update_table(n_clicks, n_intervals, table_ids):
    updated_data = [get_data(table_id['index']) for table_id in table_ids]
    return updated_data

# 回调函数用于更新报警信息表格
@dash_app.callback(
    Output('alert-table', 'data'),
    [Input('interval-component', 'n_intervals')]
)
def update_alert_messages(n_intervals):
    # 返回报警信息作为表格数据
    return alert_messages

# Flask路由接收报警信息
@app.route('/alert', methods=['POST'])
def receive_alert():
    alert_message = request.json.get('message', 'No message provided')
    velocity = request.json.get('velocity', 'Unknown')
    timestamp = request.json.get('timestamp', 'Unknown')

    # 创建完整的报警信息字典
    alert_entry = {
        'timestamp': timestamp,
        'message': alert_message,
        
    }

    # 将新的报警信息插入到列表的最前面
    alert_messages.insert(0, alert_entry)

    # 保持列表长度不超过10
    if len(alert_messages) > 5:
        alert_messages.pop()

    return {'status': 'success', 'message': 'Alert received'}

# Flask应用路由
@app.route('/')
def index():
    return dash_app.index()

if __name__ == '__main__':
    app.run(host='192.168.1.204', port=5000, debug=True)