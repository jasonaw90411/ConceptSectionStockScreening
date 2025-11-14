import os
import json
import logging
from datetime import datetime
import requests
import pandas as pd
from typing import List, Dict
from io import StringIO

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('concept_section_screening.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_top_concept_sections() -> List[Dict]:
    """
    获取前十概念板块数据
    
    Returns:
        List[Dict]: 前十概念板块数据列表
    """
    logger.info("开始获取概念板块资金流向排行前十")
    
    try:
        # 使用东方财富API接口获取概念板块数据
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': 1,  # 页码
            'pz': 20,  # 每页数量
            'po': 1,  # 排序方式
            'np': 1,  # 
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': 2,
            'invt': 2,
            'fid': 'f62',  # 主力净流入排序
            'fs': 'm:90 t:3',  # 概念板块
            'fields': 'f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124',
            '_': '1639125329869'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Referer': 'https://data.eastmoney.com/',
        }
        
        # 添加重试机制
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"第{attempt + 1}次请求失败，重试中...")
                    import time
                    time.sleep(2)
                else:
                    raise e
        
        data = response.json()
        
        if data.get('rc') != 0 or not data.get('data', {}).get('diff'):
            logger.error("API返回数据格式错误")
            return []
        
        # 处理API数据
        concepts = []
        for item in data['data']['diff'][:10]:  # 只取前十
            concept = {
                'code': item.get('f12', ''),
                'name': item.get('f14', ''),
                'current_price': float(item.get('f2', 0)),
                'change_rate': float(item.get('f3', 0)),
                'main_inflow': float(item.get('f62', 0)),
                'main_inflow_ratio': float(item.get('f184', 0)),
                'super_large_inflow': float(item.get('f66', 0)),
                'super_large_inflow_ratio': float(item.get('f69', 0)),
                'large_inflow': float(item.get('f72', 0)),
                'large_inflow_ratio': float(item.get('f75', 0)),
                'medium_inflow': float(item.get('f78', 0)),
                'medium_inflow_ratio': float(item.get('f81', 0)),
                'small_inflow': float(item.get('f84', 0)),
                'small_inflow_ratio': float(item.get('f87', 0)),
                'max_stock': item.get('f204', ''),
                'max_stock_code': item.get('f205', ''),
                'datetime': item.get('f124', '')
            }
            concepts.append(concept)
        
        logger.info(f"成功获取前十概念板块: {[c['name'] for c in concepts]}")
        
        # 保存数据
        save_concept_data(concepts)
        
        return concepts
        
    except requests.RequestException as e:
        logger.error(f"获取概念板块数据失败: {e}")
        return []
        
    logger.info(f"找到概念板块表格，形状: {concept_table.shape}")
    logger.info(f"表格列名: {list(concept_table.columns)}")
    
    # 处理表格数据
    top_concepts = process_concept_table(concept_table)
    
    # 只取前十
    top_10 = top_concepts[:10]
    
    logger.info(f"成功获取前十概念板块: {[c['name'] for c in top_10]}")
    
    # 保存数据
    save_concept_data(top_10)
    
    return top_10

def process_concept_table(table: pd.DataFrame) -> List[Dict]:
    """
    处理概念板块表格数据
    
    Args:
        table: pandas DataFrame
        
    Returns:
        List[Dict]: 处理后的概念板块数据
    """
    concepts = []
    
    # 重命名列名，使其更标准化
    table = standardize_column_names(table)
    
    # 遍历表格行
    for index, row in table.iterrows():
        try:
            # 跳过表头行或空行
            if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() == '':
                continue
                
            # 提取数据
            concept_data = extract_concept_data(row)
            
            if concept_data and concept_data['name']:
                concepts.append(concept_data)
                
        except Exception as e:
            logger.warning(f"处理表格行失败: {e}")
            continue
    
    return concepts

def standardize_column_names(table: pd.DataFrame) -> pd.DataFrame:
    """
    标准化列名
    
    Args:
        table: pandas DataFrame
        
    Returns:
        pd.DataFrame: 列名标准化后的表格
    """
    # 创建列名映射
    column_mapping = {}
    
    for col in table.columns:
        col_str = str(col).strip()
        
        # 名称列
        if any(keyword in col_str for keyword in ['名称', '板块', '概念']):
            column_mapping[col] = 'name'
        
        # 涨跌幅列
        elif any(keyword in col_str for keyword in ['涨跌幅', '涨跌']):
            column_mapping[col] = 'change_rate'
        
        # 主力净流入列
        elif any(keyword in col_str for keyword in ['主力净流入', '主力流入']):
            column_mapping[col] = 'main_inflow'
        
        # 超大单净流入列
        elif any(keyword in col_str for keyword in ['超大单净流入', '超大单流入']):
            column_mapping[col] = 'super_large_inflow'
        
        # 大单净流入列
        elif any(keyword in col_str for keyword in ['大单净流入', '大单流入']):
            column_mapping[col] = 'large_inflow'
        
        # 中单净流入列
        elif any(keyword in col_str for keyword in ['中单净流入', '中单流入']):
            column_mapping[col] = 'medium_inflow'
        
        # 小单净流入列
        elif any(keyword in col_str for keyword in ['小单净流入', '小单流入']):
            column_mapping[col] = 'small_inflow'
        
        # 主力净流入最大股列
        elif any(keyword in col_str for keyword in ['主力净流入最大股', '最大股']):
            column_mapping[col] = 'max_stock'
    
    # 重命名列
    if column_mapping:
        table = table.rename(columns=column_mapping)
    
    return table

def extract_concept_data(row: pd.Series) -> Dict:
    """
    从表格行中提取概念板块数据
    
    Args:
        row: pandas Series
        
    Returns:
        Dict: 概念板块数据
    """
    try:
        # 获取名称
        name = ''
        if 'name' in row.index:
            name = str(row['name']).strip() if not pd.isna(row['name']) else ''
        else:
            # 如果没有name列，尝试第一列
            name = str(row.iloc[1]).strip() if len(row) > 1 else ''
        
        # 跳过无效名称
        if not name or name in ['名称', '板块', '概念', 'nan']:
            return {}
        
        # 获取涨跌幅
        change_rate = 0.0
        if 'change_rate' in row.index:
            change_rate = parse_percentage(str(row['change_rate']))
        elif len(row) > 2:
            change_rate = parse_percentage(str(row.iloc[2]))
        
        # 获取主力净流入
        main_inflow = 0.0
        if 'main_inflow' in row.index:
            main_inflow = parse_money_value(str(row['main_inflow']))
        
        # 获取超大单净流入
        super_large_inflow = 0.0
        if 'super_large_inflow' in row.index:
            super_large_inflow = parse_money_value(str(row['super_large_inflow']))
        elif len(row) > 4:
            super_large_inflow = parse_money_value(str(row.iloc[4]))
        
        # 获取大单净流入
        large_inflow = 0.0
        if 'large_inflow' in row.index:
            large_inflow = parse_money_value(str(row['large_inflow']))
        elif len(row) > 6:
            large_inflow = parse_money_value(str(row.iloc[6]))
        
        # 获取中单净流入
        medium_inflow = 0.0
        if 'medium_inflow' in row.index:
            medium_inflow = parse_money_value(str(row['medium_inflow']))
        
        # 获取小单净流入
        small_inflow = 0.0
        if 'small_inflow' in row.index:
            small_inflow = parse_money_value(str(row['small_inflow']))
        
        # 获取主力净流入最大股
        max_stock = ''
        if 'max_stock' in row.index:
            max_stock = str(row['max_stock']).strip() if not pd.isna(row['max_stock']) else ''
        elif len(row) > 9:
            max_stock = str(row.iloc[9]).strip()
        
        return {
            'name': name,
            'change_rate': change_rate,
            'main_inflow': main_inflow,
            'super_large_inflow': super_large_inflow,
            'large_inflow': large_inflow,
            'medium_inflow': medium_inflow,
            'small_inflow': small_inflow,
            'max_stock': max_stock,
            'total_inflow': super_large_inflow + large_inflow  # 超大单+大单净流入
        }
        
    except Exception as e:
        logger.warning(f"提取概念数据失败: {e}")
        return {}

def parse_percentage(value: str) -> float:
    """
    解析百分比值
    
    Args:
        value: 字符串值
        
    Returns:
        float: 百分比数值
    """
    try:
        # 移除百分号并转换为浮点数
        value = value.replace('%', '').strip()
        return float(value)
    except:
        return 0.0

def parse_money_value(value: str) -> float:
    """
    解析金额值（亿元）
    
    Args:
        value: 字符串值
        
    Returns:
        float: 金额数值（亿元）
    """
    try:
        # 移除单位并转换为浮点数
        value = value.replace('亿', '').replace('万', '').strip()
        # 如果是万元，转换为亿元
        if '万' in str(value):
            return float(value) / 10000
        return float(value)
    except:
        return 0.0

def save_concept_data(concepts: List[Dict]):
    """
    保存概念板块数据到JSON文件
    
    Args:
        concepts: 概念板块数据列表
    """
    try:
        data = {
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'concepts': concepts
        }
        
        with open('concept_section_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"概念板块数据已保存到 concept_section_data.json")
        
        # 更新历史数据
        update_historical_data(concepts)
        
    except Exception as e:
        logger.error(f"保存概念板块数据失败: {e}")

def update_historical_data(concepts: List[Dict]):
    """
    更新历史数据，保存最近10天的概念板块信息
    
    Args:
        concepts: 概念板块数据列表
    """
    try:
        # 读取历史数据
        history_file = 'concept_section_history.json'
        if os.path.exists(history_file):
            with open(history_file, 'r', encoding='utf-8') as f:
                historical_data = json.load(f)
        else:
            historical_data = {'historical_data': {}}
        
        # 获取当前日期
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 提取概念名称列表
        concept_names = [concept['name'] for concept in concepts]
        
        # 添加今日数据
        historical_data['historical_data'][current_date] = {
            'date': current_date,
            'concepts': concept_names,
            'count': len(concept_names)
        }
        
        # 只保留最近10天的数据
        dates = sorted(historical_data['historical_data'].keys())
        if len(dates) > 10:
            # 删除最早的数据
            for old_date in dates[:-10]:
                del historical_data['historical_data'][old_date]
                logger.info(f"删除历史数据: {old_date}")
        
        # 保存更新后的历史数据
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(historical_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"历史数据已更新，共保存 {len(historical_data['historical_data'])} 天的数据")
        
        # 生成历史统计并更新HTML
        generate_historical_statistics(historical_data)
        
    except Exception as e:
        logger.error(f"更新历史数据失败: {e}")

def generate_historical_statistics(historical_data: Dict):
    """
    生成历史统计数据并更新HTML文件
    
    Args:
        historical_data: 历史数据字典
    """
    try:
        # 统计概念板块出现次数
        concept_count = {}
        
        # 只统计最近5天的数据
        dates = sorted(historical_data['historical_data'].keys())[-5:]
        
        for date in dates:
            concepts = historical_data['historical_data'][date]['concepts']
            for concept in concepts:
                concept_count[concept] = concept_count.get(concept, 0) + 1
        
        # 按出现次数排序，取前10
        sorted_concepts = sorted(concept_count.items(), key=lambda x: x[1], reverse=True)[:10]
        
        logger.info(f"历史统计完成，前5天概念板块出现次数统计: {sorted_concepts}")
        
        # 更新HTML报告
        update_html_report(sorted_concepts, historical_data)
        
    except Exception as e:
        logger.error(f"生成历史统计数据失败: {e}")

def update_html_report(sorted_concepts: List, historical_data: Dict):
    """
    更新HTML报告文件
    
    Args:
        sorted_concepts: 排序后的概念板块列表
        historical_data: 历史数据字典
    """
    try:
        # 读取当前的概念板块数据
        current_data_file = 'concept_section_data.json'
        if os.path.exists(current_data_file):
            with open(current_data_file, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
        else:
            current_data = {'concepts': []}
        
        # 生成HTML内容
        html_content = generate_html_content(current_data, sorted_concepts, historical_data)
        
        # 保存HTML文件
        with open('concept_section_report.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info("HTML报告已更新")
        
    except Exception as e:
        logger.error(f"更新HTML报告失败: {e}")

def generate_html_content(current_data: Dict, sorted_concepts: List, historical_data: Dict) -> str:
    """
    生成HTML内容
    
    Args:
        current_data: 当前概念板块数据
        sorted_concepts: 排序后的历史概念板块
        historical_data: 历史数据字典
        
    Returns:
        str: HTML内容
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>概念板块资金流向报告</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #2c3e50, #34495e);
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 1.8em;
            font-weight: 300;
        }}
        .header p {{
            margin: 8px 0 0 0;
            opacity: 0.8;
            font-size: 0.9em;
        }}
        .content {{
            padding: 20px;
        }}
        .section {{
            margin-bottom: 20px;
        }}
        .section h2 {{
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 6px;
            margin-bottom: 12px;
            font-size: 1.2em;
        }}
        .dashboard {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .section {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}
        .section h2 {{
            color: #2c3e50;
            margin: 0 0 12px 0;
            font-size: 1.2em;
            border-bottom: 2px solid #3498db;
            padding-bottom: 6px;
        }}
        .table-container {{
            overflow-x: auto;
        }}
        .concept-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85em;
            background: white;
            border-radius: 6px;
            overflow: hidden;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }}
        .concept-table th {{
            background: linear-gradient(135deg, #3498db, #2980b9);
            color: white;
            padding: 8px 10px;
            text-align: left;
            font-weight: 600;
            font-size: 0.9em;
        }}
        .concept-table td {{
            padding: 6px 10px;
            border-bottom: 1px solid #ecf0f1;
        }}
        .concept-table tr:hover {{
            background-color: #f1f2f6;
        }}
        .positive {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .negative {{
            color: #27ae60;
            font-weight: bold;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #f39c12, #e67e22);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            font-size: 1.2em;
        }}
        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
        }}
        .history-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        .history-table th {{
            background: linear-gradient(135deg, #9b59b6, #8e44ad);
            color: white;
            padding: 12px;
            text-align: left;
        }}
        .history-table td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        .rank-1 {{ background-color: #f1c40f; color: #2c3e50; font-weight: bold; }}
        .rank-2 {{ background-color: #e67e22; color: white; }}
        .rank-3 {{ background-color: #e74c3c; color: white; }}
        .footer {{
            background: #34495e;
            color: white;
            text-align: center;
            padding: 20px;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>概念板块资金流向分析</h1>
            <p>更新时间: {current_time}</p>
        </div>
        
        <div class="content">
            <div class="dashboard">
                <!-- 当日概念板块数据 -->
                <div class="section">
                    <h2>📊 当日概念板块前十</h2>
                    <div class="table-container">
                        <table class="concept-table">
                            <thead>
                                <tr>
                                    <th>排名</th>
                                    <th>概念板块</th>
                                    <th>涨跌幅(%)</th>
                                    <th>主力净流入(万)</th>
                                    <th>超大单(万)</th>
                                    <th>大单(万)</th>
                                    <th>龙头股</th>
                                </tr>
                            </thead>
                            <tbody>
"""
    
    # 添加当前数据行
    for i, concept in enumerate(current_data.get('concepts', []), 1):
        change_class = 'positive' if concept.get('change_rate', 0) > 0 else 'negative'
        html += f"""
                                <tr>
                                    <td>{i}</td>
                                    <td><strong>{concept.get('name', '')}</strong></td>
                                    <td class="{change_class}">{concept.get('change_rate', 0):.2f}%</td>
                                    <td>{concept.get('main_inflow', 0)/10000:.0f}</td>
                                    <td>{concept.get('super_large_inflow', 0)/10000:.0f}</td>
                                    <td>{concept.get('large_inflow', 0)/10000:.0f}</td>
                                    <td>{concept.get('max_stock', '')}</td>
                                </tr>
"""
    
    html += """
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <!-- 历史统计数据 -->
                <div class="section">
                    <h2>📈 前5天概念频率统计</h2>
                    <div class="table-container">
                        <table class="history-table">
                            <thead>
                                <tr>
                                    <th>排名</th>
                                    <th>概念板块</th>
                                    <th>出现次数</th>
                                    <th>频率</th>
                                </tr>
                            </thead>
                            <tbody>
"""
    
    # 添加历史统计行
    total_days = min(5, len(historical_data.get('historical_data', {})))
    for i, (concept, count) in enumerate(sorted_concepts, 1):
        frequency = f"{(count/total_days)*100:.1f}%" if total_days > 0 else "0%"
        rank_class = f"rank-{i}" if i <= 3 else ""
        html += f"""
                        <tr class="{rank_class}">
                            <td>{i}</td>
                            <td><strong>{concept}</strong></td>
                            <td>{count}</td>
                            <td>{frequency}</td>
                        </tr>
"""
    
    html += """
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            
            <!-- 数据概览 -->
            <div class="section">
                <h2>📋 数据概览</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <h3>历史数据天数</h3>
                        <div class="value">{len(historical_data.get('historical_data', {}))}</div>
                    </div>
                    <div class="stat-card">
                        <h3>统计天数</h3>
                        <div class="value">{total_days}</div>
                    </div>
                    <div class="stat-card">
                        <h3>当前概念板块</h3>
                        <div class="value">{len(current_data.get('concepts', []))}</div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>© 2024 概念板块资金流向分析系统 | 数据更新时间: {current_time}</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html

def main():
    """
    主函数
    """
    logger.info("开始概念板块筛选")
    
    # 获取前十概念板块
    top_concepts = get_top_concept_sections()
    
    if top_concepts:
        logger.info(f"成功获取 {len(top_concepts)} 个概念板块")
        for i, concept in enumerate(top_concepts, 1):
            total_inflow = concept.get('super_large_inflow', 0) + concept.get('large_inflow', 0)
            logger.info(f"{i}. {concept['name']}: 涨跌幅 {concept['change_rate']:.2f}%, "
                       f"主力净流入 {concept['main_inflow']:.2f}亿, "
                       f"超大单+大单 {total_inflow:.2f}亿")
    else:
        logger.error("未能获取概念板块数据")

if __name__ == "__main__":
    main()