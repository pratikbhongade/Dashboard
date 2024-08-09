import pyodbc
import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output, State
from datetime import datetime, timedelta
import os
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from io import BytesIO
import time
import win32com.client as win32
from multiprocessing import Process, Queue
import plotly.express as px
import plotly.graph_objects as go
import base64

# Path to your logo image
logo_path = 'C:\\Aspire_Dashboard\\Aspire.png'

# Encode the image to base64
with open(logo_path, 'rb') as f:
    logo_base64 = base64.b64encode(f.read()).decode('ascii')

# Function to get the last business day
def get_last_business_day():
    today = datetime.today()
    if today.weekday() == 0:  # Monday
        last_business_day = today - timedelta(days=3)
    elif today.weekday() == 6:  # Sunday
        last_business_day = today - timedelta(days=2)
    else:  # Any other day (Tuesday to Saturday)
        last_business_day = today - timedelta(days=1)
    
    # Ensure the date is within the current month
    if last_business_day.month != today.month:
        last_business_day = today.replace(day=1) - timedelta(days=1)
        while last_business_day.weekday() >= 5:  # Skip weekends
            last_business_day -= timedelta(days=1)
    
    return last_business_day

# Function to get the last 5 business days
def get_last_5_business_days(selected_date):
    selected_date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
    business_days = []
    current_day = selected_date_obj
    while len(business_days) < 5:
        if current_day.weekday() < 5:  # Monday to Friday are business days
            business_days.append(current_day.strftime('%Y-%m-%d'))
        current_day -= timedelta(days=1)
    return business_days

# Get the default date
default_date = get_last_business_day().strftime('%Y-%m-%d')

# Function to fetch data
def fetch_data(selected_date):
    conn_str = (
        r'DRIVER={SQL Server};'
        r'SERVER=SDC01ASRSQPD01S\PSQLINST01;'
        r'DATABASE=ASPIRE;'
        r'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)

    query = f"""
    SELECT 
        CASE 
            WHEN DATEPART(hour, JSH.StartTime) < 14 THEN CONVERT(varchar, DATEADD(day, -1, JSH.StartTime), 23) 
            ELSE CONVERT(varchar, JSH.StartTime, 23) 
        END as ProcessingDate, 
        JSJ.JobStreamJoboid as Joboid, 
        JSJ.Name as JobName,
        CONVERT(datetime, JSH.StartTime AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS [StartTime], 
        CONVERT(datetime, JSH.EndTime AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS [EndTime], 
        JSH.Status, 
        JSH.Message 
    FROM JobStreamTaskHistory JSH
    LEFT JOIN JobStreamTask JST ON JSH.JobStreamTaskOid = JST.JobStreamTaskoid 
    JOIN JobStreamJob JSJ ON JSJ.JobStreamJoboid = JST.JobStreamJoboid
    WHERE 
        CASE 
            WHEN DATEPART(hour, JSH.StartTime) < 14 THEN CONVERT(varchar, DATEADD(day, -1, JSH.StartTime), 23) 
            ELSE CONVERT(varchar, JSH.StartTime, 23) 
        END = '{selected_date}'
    ORDER BY StartTime ASC
    """
    df = pd.read_sql(query, conn)

    query_30_days = """
    SELECT 
        CONVERT(varchar, JSH.StartTime, 23) as ProcessingDate, 
        JSH.Status,
        JSJ.Name as JobName,
        CONVERT(datetime, JSH.StartTime AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS [StartTime],
        CONVERT(datetime, JSH.EndTime AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS [EndTime],
        JSH.Message
    FROM JobStreamTaskHistory JSH
    LEFT JOIN JobStreamTask JST ON JSH.JobStreamTaskOid = JST.JobStreamTaskoid 
    JOIN JobStreamJob JSJ ON JSJ.JobStreamJoboid = JST.JobStreamJoboid
    WHERE JSH.StartTime >= DATEADD(day, -30, GETDATE())
    """
    df_30_days = pd.read_sql(query_30_days, conn)

    query_job_duration = f"""
    SELECT 
        CONVERT(varchar, JSH.StartTime, 23) as ProcessingDate, 
        JSJ.Name as JobName,
        DATEDIFF(SECOND, JSH.StartTime, JSH.EndTime) / 60.0 as DurationMinutes
    FROM JobStreamTaskHistory JSH
    LEFT JOIN JobStreamTask JST ON JSH.JobStreamTaskOid = JST.JobStreamTaskoid 
    JOIN JobStreamJob JSJ ON JSJ.JobStreamJoboid = JST.JobStreamJoboid
    WHERE CONVERT(varchar, JSH.StartTime, 23) = '{selected_date}'
    """
    df_job_duration = pd.read_sql(query_job_duration, conn)

    query_unlock_online = f"""
    SELECT JobName, CONVERT(datetime, EndTime) AS CompletionTime, Status 
    FROM Job_StatsVW 
    WHERE JobName = 'UnLock Online' 
    AND ProcessingDate = '{selected_date}'
    """
    df_unlock_online = pd.read_sql(query_unlock_online, conn)

    conn.close()

    return df, df_30_days, df_job_duration, df_unlock_online

# Fetch initial data
df, df_30_days, df_job_duration, df_unlock_online = fetch_data(default_date)

# Initialize the Dash app with Bootstrap CSS and suppress callback exceptions
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], assets_folder='assets', suppress_callback_exceptions=True)

# Layout of the dashboard
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.Img(src='data:image/png;base64,{}'.format(logo_base64), height='60px', id='logo'), width='auto'),
        dbc.Col(html.H1("ASPIRE DASHBOARD", className='text-center mb-4 fade-in', style={'font-weight': 'bold', 'color': '#2A3F5F', 'border-bottom': '1px solid #2A3F5F'}), width=True, className='d-flex justify-content-center align-items-center'),
        dbc.Col([
            html.Div("Pick a date 📆", className='text-center mb-2 fade-in', style={'font-weight': 'bold'}),
            dcc.DatePickerSingle(
                id='date-picker-table',
                display_format='YYYY-MM-DD',
                date=default_date,  # Default date
                className='form-control',
                style={'font-weight': 'bold'}
            ),
            html.I(className="fa fa-calendar", id="calendar-icon", style={"margin-left": "10px"}),
        ], width='auto', className='d-flex justify-content-end align-items-center fade-in'),
        dbc.Tooltip("Select a date", target="calendar-icon"),
        dbc.Tooltip("Company Logo", target="logo")
    ], className='border mb-3 align-items-center justify-content-center slide-in'),
    dbc.Tabs([
        dbc.Tab(label='Main Dashboard', tab_id='main-dashboard', children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card(
                        dbc.CardBody([
                            html.H4("Aspire Unlock Online", className='card-title'),
                            dcc.Loading(
                                id="loading-unlock-online",
                                type="default",
                                children=html.Div(id='unlock-online-table', style={'width': '50%'}, className='slide-in')
                            )
                        ]),
                        className='mb-4 border animated-card'
                    )
                ], width=12)
            ], className='border mb-3'),
            dbc.Row([
                dbc.Col([
                    html.Button("Send Email", id="send-email-button", className="btn btn-primary mt-3 pulse", style={'width': '200px'}),
                    dbc.Tooltip("Send Dashboard via Email", target="send-email-button")
                ], width=12, className='d-flex justify-content-center')
            ], className='border mt-3')
        ]),
        # New tab for Job Status Count
        dbc.Tab(label='Job Status Count', tab_id='job-status-count', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-status-bar-graph",
                        type="default",
                        children=dcc.Graph(id='status-bar-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        # New tab for Failure Trend Over the Last 30 Days
        dbc.Tab(label='Failure Trend', tab_id='failure-trend', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-failure-trend-graph",
                        type="default",
                        children=dcc.Graph(id='failure-trend-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        dbc.Tab(label='Job Duration Analysis', tab_id='job-duration', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-job-duration-graph",
                        type="default",
                        children=dcc.Graph(id='job-duration-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        dbc.Tab(label='Performance Metrics', tab_id='performance-metrics', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-performance-metrics-graph",
                        type="default",
                        children=dcc.Graph(id='performance-metrics-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        dbc.Tab(label='Anomaly Detection', tab_id='anomaly-detection', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-anomaly-detection-graph",
                        type="default",
                        children=dcc.Graph(id='anomaly-detection-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        dbc.Tab(label='Time to Recovery', tab_id='time-to-recovery', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-time-to-recovery-graph",
                        type="default",
                        children=dcc.Graph(id='time-to-recovery-graph', className='fade-in')
                    )
                ], width=12)
            ], className='border mt-3')
        ]),
        # Time Difference Analysis is now only in its own tab
        dbc.Tab(label='Time Difference', tab_id='time-difference', children=[
            dbc.Row([
                dbc.Col([
                    dcc.Loading(
                        id="loading-time-difference-graph",
                        type="default",
                        children=dcc.Graph(id='time-difference-graph', className='fade-in')
                    )
                ], width=9),
                dbc.Col([
                    dcc.Loading(
                        id="loading-time-difference-table",
                        type="default",
                        children=html.Div(id='time-difference-table', style={'font-size': '14px'}, className='fade-in')
                    )
                ], width=3)
            ], className='border mt-3')
        ])
    ]),
    dcc.ConfirmDialog(
        id='confirm-dialog',
        message='Email sent successfully!',
    ),
], fluid=True, className='p-4 bg-light rounded-3 shadow')

# Update callback to remove status filter input and adjust logic accordingly
@app.callback(
    [Output('unlock-online-table', 'children'),
     Output('job-table-container', 'children'),
     Output('status-bar-graph', 'figure'),
     Output('failure-trend-graph', 'figure'),
     Output('time-difference-graph', 'figure'),
     Output('time-difference-table', 'children'),
     Output('job-duration-graph', 'figure'),
     Output('performance-metrics-graph', 'figure'),
     Output('anomaly-detection-graph', 'figure'),
     Output('time-to-recovery-graph', 'figure')],
    [Input('date-picker-table', 'date')]
)
def update_dashboard(selected_date):
    now = datetime.now()
    selected_date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
    
    # Check if the selected date is a weekend, future date, or before 9 PM today
    if selected_date_obj.weekday() >= 5 or selected_date_obj > now or (selected_date == now.strftime('%Y-%m-%d') and now.hour < 21):
        if selected_date_obj.weekday() >= 5:
            message = html.Div(
                [
                    html.H4("No data available due to holidays or weekends", className='text-center text-danger slide-in')
                ]
            )
        elif selected_date_obj > now:
            message = html.Div(
                [
                    html.H4("Batch yet to start", className='text-center text-danger slide-in')
                ]
            )
        else:
            message = html.Div(
                [
                    html.H4("Batch yet to start", className='text-center text-danger slide-in')
                ]
            )

        empty_fig = px.bar()
        return message, message, empty_fig, empty_fig, empty_fig, html.Div(), empty_fig, empty_fig, empty_fig, empty_fig

    df, df_30_days, df_job_duration, df_unlock_online = fetch_data(selected_date)

    if df.empty:
        message = html.Div(
            [
                html.H4("No Data Available", className='text-center text-danger slide-in')
            ]
        )
        empty_fig = px.bar()
        return message, message, empty_fig, empty_fig, empty_fig, html.Div(), empty_fig, empty_fig, empty_fig, empty_fig

    df['StartDate'] = pd.to_datetime(df['StartTime']).dt.strftime('%Y-%m-%d')
    df['StartTime'] = pd.to_datetime(df['StartTime']).dt.strftime('%I:%M:%S %p')
    df['EndDate'] = pd.to_datetime(df['EndTime']).dt.strftime('%Y-%m-%d')
    df['EndTime'] = pd.to_datetime(df['EndTime']).dt.strftime('%I:%M:%S %p')

    df_unlock_online['CompletionTime'] = pd.to_datetime(df_unlock_online['CompletionTime']).dt.strftime('%I:%M:%S %p')

    # Job Status Count
    status_counts = df['Status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']

    fig_status = go.Figure(data=[
        go.Bar(
            x=status_counts['Count'],
            y=status_counts['Status'],
            orientation='h',
            marker=dict(
                color=status_counts['Status'].apply(lambda x: 'green' if x == 'Succeeded' else 'orange' if x == 'Succeeded with Exceptions' else 'red'),
                line=dict(color='black', width=1)
            )
        )
    ])
    fig_status.update_layout(
        title='Job Status Counts',
        xaxis_title='Count',
        yaxis_title='Status',
        template='plotly_white',
        plot_bgcolor='rgba(229,236,246,1)',
        title_font=dict(size=21, family='Arial, bold', color='rgba(42, 63, 95, 1)'),
        xaxis=dict(
            showgrid=True,
            showline=False,
            linewidth=1,
            linecolor='black',
            mirror=True,
            gridcolor='lightgrey'
        ),
        yaxis=dict(
            showgrid=True,
            showline=False,
            linewidth=1,
            linecolor='black',
            mirror=True,
            gridcolor='lightgrey',
        ),
        font=dict(size=14),
        bargap=0.4
    )

    df_30_days['ProcessingDate'] = pd.to_datetime(df_30_days['ProcessingDate']).dt.strftime('%Y-%m-%d')
    df_30_days['DurationMinutes'] = (df_30_days['EndTime'] - df_30_days['StartTime']).dt.total_seconds() / 60.0

    df_failed = df_30_days[(df_30_days['Status'] == 'Failed') & (df_30_days['JobName'] != '20. Benchmark Update')]
    failure_trend = df_failed.groupby(['ProcessingDate', 'JobName', 'StartTime', 'Message']).size().reset_index(name='Count')
    fig_trend = px.bar(failure_trend, x='ProcessingDate', y='Count', color='JobName', title='Failure Trend Over the Last 30 Days', 
                       hover_data={'StartTime': True, 'JobName': True, 'Message': True})
    fig_trend.update_layout(
        bargap=0.4,
        template='plotly_white',
        plot_bgcolor='rgba(229,236,246,1)',
        title_font=dict(size=21, family='Arial, bold', color='rgba(42, 63, 95, 1)'),
        xaxis=dict(
            showgrid=True,
            showline=False,
            linewidth=1,
            linecolor='black',
            mirror=True,
            gridcolor='lightgrey',
            tickformat='%Y-%m-%d'
        ),
        yaxis=dict(
            showgrid=True,
            showline=False,
            linewidth=1,
            linecolor='black',
            mirror=True,
            gridcolor='lightgrey',
            rangemode='tozero'
        ),
        font=dict(size=14),
        hovermode='x unified'
    )

    triad_df = df_30_days[df_30_days['JobName'] == '18. TRIAD']
    benchmark_update_df = df_30_days[df_30_days['JobName'] == '20. Benchmark Update']

    fig_time_diff = go.Figure()

    if not triad_df.empty and not benchmark_update_df.empty:
        merged_df = pd.merge(triad_df, benchmark_update_df, on='ProcessingDate', suffixes=('_TRIAD', '_Benchmark'))
        merged_df['TimeDifference'] = (merged_df['EndTime_Benchmark'] - merged_df['EndTime_TRIAD']).dt.total_seconds() / 3600

        merged_df = merged_df.sort_values('ProcessingDate', ascending=False)

        last_5_business_days = get_last_5_business_days(selected_date)
        last_5_business_days_df = merged_df[merged_df['ProcessingDate'].isin(last_5_business_days)]

        table_rows = []
        for index, row in last_5_business_days_df.iterrows():
            row_class = 'table-success' if row['ProcessingDate'] == selected_date else ''
            table_rows.append(html.Tr([
                html.Td(row['ProcessingDate']),
                html.Td(f"{row['TimeDifference']:.2f} hours")
            ], className=row_class))

        time_difference_table = dbc.Table([
            html.Thead(html.Tr([html.Th("Processing Date"), html.Th("Time Difference (hours)")]), className='bg-primary text-white'),
            html.Tbody(table_rows)
        ], bordered=True, striped=True, hover=True)

        fig_time_diff = px.line(merged_df, x='ProcessingDate', y='TimeDifference', title='Time Difference between TRIAD and Benchmark Update Jobs Over the Last 30 Days', markers=True)
        fig_time_diff.update_layout(
            xaxis_title='Processing Date',
            yaxis_title='Time Difference (hours)',
            hovermode='x unified',
            legend=dict(title="Metrics", orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template='plotly_white',
            plot_bgcolor='rgba(229,236,246,1)',
            title_font=dict(size=21, family='Arial, bold', color='rgba(42, 63, 95, 1)'),
            xaxis=dict(
                showgrid=True,
                showline=False,
                linewidth=1,
                linecolor='black',
                mirror=True,
                gridcolor='lightgrey'
            ),
            yaxis=dict(
                showgrid=True,
                showline=False,
                linewidth=1,
                linecolor='black',
                mirror=True,
                gridcolor='lightgrey',
            ),
            font=dict(size=14)
        )

    else:
        fig_time_diff = px.line(title='No data available for TRIAD or Benchmark Update jobs.')
        time_difference_table = dbc.Table([
            html.Thead(html.Tr([html.Th("Processing Date"), html.Th("Time Difference (hours)")]), className='bg-primary text-white'),
            html.Tbody([
                html.Tr([html.Td("No Data"), html.Td("No Data")])
            ])
        ], bordered=True, striped=True, hover=True)

    # Calculate average job duration per job
    df_job_duration['ProcessingDate'] = pd.to_datetime(df_job_duration['ProcessingDate']).dt.strftime('%Y-%m-%d')
    avg_duration = df_job_duration.groupby(['ProcessingDate', 'JobName'])['DurationMinutes'].mean().reset_index()
    fig_job_duration = px.line(avg_duration, x='ProcessingDate', y='DurationMinutes', color='JobName', title='Average Job Duration Over Time')

    # Performance metrics comparison
    performance_metrics = df_30_days.groupby('JobName').agg(
        AvgDuration=('DurationMinutes', 'mean'),
        SuccessRate=('Status', lambda x: (x != 'Failed').mean() * 100),
        Frequency=('JobName', 'count')
    ).reset_index()
    fig_performance_metrics = px.box(performance_metrics.melt(id_vars='JobName'), x='JobName', y='value', color='variable', title='Performance Metrics Comparison')

    # Anomaly detection (using z-score)
    df_30_days['DurationZScore'] = (df_30_days['DurationMinutes'] - df_30_days['DurationMinutes'].mean()) / df_30_days['DurationMinutes'].std()
    anomalies = df_30_days[df_30_days['DurationZScore'].abs() > 2]
    fig_anomaly_detection = px.scatter(anomalies, x='StartTime', y='DurationMinutes', color='JobName', title='Anomaly Detection in Job Durations')

    # Time to recovery from failures
    df_30_days['RecoveryTime'] = df_30_days.groupby('JobName')['EndTime'].diff().dt.total_seconds() / 3600
    recovery_data = df_30_days[df_30_days['Status'] == 'Failed'].groupby('ProcessingDate')['RecoveryTime'].mean().reset_index()
    fig_recovery = px.bar(recovery_data, x='ProcessingDate', y='RecoveryTime', title='Time to Recovery from Job Failures')

    return (html.Div(), job_table, fig_status, fig_trend, fig_time_diff, time_difference_table, 
            fig_job_duration, fig_performance_metrics, fig_anomaly_detection, fig_recovery)

def run_dash_app(queue):
    app.run_server(debug=True, port=8050, use_reloader=False)
    queue.put("Dash app stopped")

def run_dashboard():
    queue = Queue()
    dash_process = Process(target=run_dash_app, args=(queue,))
    dash_process.start()

    time.sleep(10)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    chrome_driver_path = "C:\\chromedriver_win64\\chromedriver.exe"
    webdriver_service = ChromeService(executable_path=chrome_driver_path)
    
    try:
        driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    except Exception as e:
        raise

    driver.get("http://127.0.0.1:8050/")
    time.sleep(5)

    return driver, dash_process, queue

def capture_full_page_screenshot(driver, file_path):
    total_width = driver.execute_script("return document.body.scrollWidth")
    total_height = driver.execute_script("return document.body.scrollHeight")
    viewport_height = driver.execute_script("return window.innerHeight")

    driver.set_window_size(total_width, viewport_height)
    time.sleep(2)

    screenshots = []

    for i in range(0, total_height, viewport_height):
        if i + viewport_height > total_height:
            viewport_height = total_height - i
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(1)
            driver.set_window_size(total_width, viewport_height)
        else:
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(1)
        
        screenshot = driver.get_screenshot_as_png()
        screenshots.append(Image.open(BytesIO(screenshot)))

    stitched_image = Image.new('RGB', (total_width, total_height))
    y_offset = 0
    for screenshot in screenshots:
        stitched_image.paste(screenshot, (0, y_offset))
        y_offset += screenshot.size[1]

    stitched_image.save(file_path)

def send_email_with_screenshot(image_path, processing_date, benchmark_end_time):
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = 'Pratik_Bhongade@Keybank.com'
    mail.Subject = 'Aspire Dashboard'
    
    with open(image_path, 'rb') as f:
        image_data = f.read()
        image_cid = 'dashboard_image'
    
    benchmark_end_time_formatted = benchmark_end_time.strftime('%I:%M %p')
    
    mail.HTMLBody = f'''
    <p>Hi All,</p>
    <p>Please find the status of Aspire Nightly Batch - <strong>{processing_date}</strong></p>
    <p><strong>Highlight:</strong></p>
    <ul>
        <li>Aspire Online Availability at <strong>{benchmark_end_time_formatted}</strong></li>
    </ul>
    <p><u><strong>ASPIRE DASHBOARD</strong></u>:</p>
    <img src="cid:{image_cid}" width="800">
    <p>Thanks,</p>
    <p>Pratik Bhongade<br>
    Jr. Software Engineer<br>
    Pune, India<br>
    KEF<br>
    Pratik_Bhongade@key.com</p>
    '''

    attachment = mail.Attachments.Add(image_path)
    attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", image_cid)

    mail.Send()

@app.callback(
    [Output('send-email-button', 'n_clicks'), Output('confirm-dialog', 'displayed')],
    [Input('send-email-button', 'n_clicks')],
    [State('date-picker-table', 'date')]
)
def handle_send_email(n_clicks, selected_date):
    if n_clicks is not None:
        driver, dash_process, queue = run_dashboard()

        image_path = r"C:\Aspire_Dashboard\dashboard.png"
        os.makedirs(os.path.dirname(image_path), exist_ok=True)
        capture_full_page_screenshot(driver, image_path)

        processing_date = selected_date
        benchmark_end_time = df[df['JobName'] == '20. Benchmark Update']['EndTime'].max()

        send_email_with_screenshot(image_path, processing_date, benchmark_end_time)

        driver.quit()
        dash_process.terminate()

        return None, True

    return None, False

def main():
    driver, dash_process, queue = run_dashboard()

    print("Dashboard is running. Press Ctrl+C to stop.")
    try:
        while True:
            if not queue.empty():
                message = queue.get()
                if message == "Dash app stopped":
                    break
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
