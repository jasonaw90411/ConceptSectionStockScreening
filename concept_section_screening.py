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
        
    except requests.RequestException as e:
        logger.error(f"获取概念板块数据失败: {e}")
        return []
    except Exception as e:
        logger.error(f"处理概念板块数据时发生错误: {e}")
        return []

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
        
    except Exception as e:
        logger.error(f"保存数据失败: {e}")

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