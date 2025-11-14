import requests
import pandas as pd
import json
import time
import re
from datetime import datetime
from typing import List, Dict
import logging
from bs4 import BeautifulSoup
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 尝试导入akshare库，如果未安装则使用备用方法
AKSHARE_AVAILABLE = False
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    logger.info("akshare库可用")
except ImportError:
    logger.warning("akshare库未安装，将使用备用方法")

def scrape_lianban_stocks():
    """
    从东方财富网爬取连板股票数据
    """
    logger.info("开始爬取连板股票数据...")
    
    # 优先使用akshare库获取数据
    if AKSHARE_AVAILABLE:
        try:
            stocks = scrape_lianban_with_akshare()
            if stocks:
                return stocks
        except Exception as e:
            logger.warning(f"akshare获取数据失败: {e}，使用备用方法")
    
    # 备用方法1：使用API接口
    try:
        stocks = scrape_lianban_stocks_api()
        if stocks:
            return stocks
    except Exception as e:
        logger.warning(f"API接口获取数据失败: {e}")
    
    # 备用方法2：网页爬取
    try:
        stocks = scrape_lianban_from_webpage()
        if stocks:
            return stocks
    except Exception as e:
        logger.error(f"所有方法都失败: {e}")
        return []

def scrape_lianban_with_akshare():
    """
    使用akshare库获取涨停股票数据
    """
    logger.info("使用akshare库获取涨停股票数据...")
    
    try:
        # 获取A股实时数据
        logger.info("获取A股实时数据...")
        stock_data = ak.stock_zh_a_spot_em()
        
        if stock_data.empty:
            logger.warning("akshare未返回A股数据")
            return []
        
        # 筛选涨停股票（涨跌幅 >= 9.9%）
        limit_up_stocks = stock_data[stock_data['涨跌幅'] >= 9.9]
        
        if limit_up_stocks.empty:
            logger.warning("未找到涨停股票")
            return []
        
        logger.info(f"找到{len(limit_up_stocks)}只涨停股票")
        
        stocks = []
        for _, row in limit_up_stocks.iterrows():
            stock = {
                'code': str(row.get('代码', '')),
                'name': row.get('名称', ''),
                'price': float(row.get('最新价', 0)),
                'change_rate': float(row.get('涨跌幅', 0)),
                'lianban_days': 1,  # 默认1天，需要额外信息
                'is_new_stock': False,
                'first_limit_time': '',
                'last_limit_time': '',
                'limit_type': '涨停',
                'fund_inflow': 0,  # 实时数据不包含资金流向
                'market_value': float(row.get('流通市值', 0)),
                'turnover_rate': float(row.get('换手率', 0)),
                'pe_ratio': float(row.get('市盈率', 0)),
                'is_st': 'ST' in str(row.get('名称', '')),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            stocks.append(stock)
        
        logger.info(f"akshare成功获取{len(stocks)}只涨停股票")
        return stocks
        
    except Exception as e:
        logger.error(f"akshare获取实时数据失败: {e}")
        
        # 备用方法：尝试获取历史涨停数据
        try:
            logger.info("尝试获取历史涨停数据...")
            # 获取历史涨停股票数据（可能需要调整日期）
            from datetime import timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            
            # 尝试获取昨日涨停数据
            zt_data = ak.stock_zh_a_hist_min_em(symbol="000001", start_date=yesterday, end_date=yesterday)
            
            if not zt_data.empty:
                logger.info(f"获取到历史数据，但无法直接获取连板信息")
                
            return []
            
        except Exception as e2:
            logger.error(f"akshare获取历史数据也失败: {e2}")
            return []
        
        stocks = []
        for _, row in today_zt.iterrows():
            # 过滤连板股票（连续涨停天数 > 1）
            if row.get('连续涨停天数', 0) > 0:  # 包含连板1天的股票
                stock = {
                    'code': str(row.get('代码', '')),
                    'name': row.get('名称', ''),
                    'price': float(row.get('最新价', 0)),
                    'change_rate': float(row.get('涨跌幅', 0)),
                    'lianban_days': int(row.get('连续涨停天数', 1)),
                    'is_new_stock': False,  # akshare会标识新股
                    'first_limit_time': row.get('首次涨停时间', ''),
                    'last_limit_time': row.get('最后涨停时间', ''),
                    'limit_type': row.get('涨停类型', ''),
                    'fund_inflow': float(row.get('封板资金', 0)),
                    'market_value': float(row.get('流通市值', 0)),
                    'turnover_rate': float(row.get('换手率', 0)),
                    'pe_ratio': float(row.get('动态市盈率', 0)),
                    'is_st': False,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                stocks.append(stock)
        
        logger.info(f"akshare成功获取{len(stocks)}只连板股票")
        return stocks
        
    except Exception as e:
        logger.error(f"akshare获取数据失败: {e}")
        return []

def scrape_lianban_from_webpage():
    """
    从东方财富网页爬取连板股票数据
    """
    try:
        # 东方财富网昨日连板板块页面
        url = "https://data.eastmoney.com/bkzj/BK1051.html"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://data.eastmoney.com/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        # 发送请求获取页面
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # 解析HTML页面
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 查找JavaScript中的股票数据
        stocks = []
        
        # 查找包含股票数据的JavaScript
        scripts = soup.find_all('script')
        logger.info(f"找到{len(scripts)}个script标签")
        
        # 查找包含股票数据的JavaScript变量
        stock_data_found = False
        for script in scripts:
            if script.string and ('data' in script.string or '股票' in script.string or '代码' in script.string):
                script_content = script.string
                logger.info(f"找到可能包含数据的script，长度: {len(script_content)}")
                
                # 尝试提取JavaScript中的股票数据
                # 查找类似 var data = [...] 的模式
                data_pattern = r'var\s+\w*data\w*\s*=\s*(\[.*?\]);'
                matches = re.findall(data_pattern, script_content, re.DOTALL)
                
                if matches:
                    logger.info(f"找到{len(matches)}个数据变量")
                    for match in matches:
                        try:
                            # 尝试解析JSON数据
                            data = json.loads(match)
                            if isinstance(data, list) and len(data) > 0:
                                logger.info(f"解析到数据列表，长度: {len(data)}")
                                # 检查是否包含股票数据
                                if self._is_stock_data(data):
                                    stocks.extend(self._parse_stock_list(data))
                                    stock_data_found = True
                        except json.JSONDecodeError:
                            continue
                
                # 查找其他可能的数据格式
                if not stock_data_found:
                    # 尝试查找包含股票代码的模式
                    stock_code_pattern = r'\[\s*\{\s*"[^"]*code[^"]*"\s*:\s*"\d{6}"'
                    if re.search(stock_code_pattern, script_content):
                        logger.info("找到可能的股票数据格式")
                        # 尝试提取和解析
                        self._extract_stock_data_from_script(script_content, stocks)
                        
        if not stock_data_found:
            logger.info("未在JavaScript中找到股票数据，尝试直接调用数据接口...")
            # 使用备用方法：调用数据接口
            return scrape_lianban_stocks_api()
        
        logger.info(f"成功获取{len(stocks)}只连板股票数据")
        return stocks
        
    except requests.RequestException as e:
        logger.error(f"网络请求失败: {e}")
        return []
    except Exception as e:
        logger.error(f"网页爬取失败: {e}")
        return []

def _is_stock_data(self, data):
    """检查数据是否为股票数据"""
    if not isinstance(data, list) or len(data) == 0:
        return False
    
    # 检查第一个元素是否包含股票相关字段
    first_item = data[0]
    if isinstance(first_item, dict):
        stock_fields = ['code', 'codes', '股票代码', '代码', 'symbol', 'f12']
        return any(field in first_item for field in stock_fields)
    
    return False

def _parse_stock_list(self, data):
    """解析股票数据列表"""
    stocks = []
    for item in data:
        if isinstance(item, dict):
            stock = self._parse_stock_item(item)
            if stock:
                stocks.append(stock)
    return stocks

def _parse_stock_item(self, item):
    """解析单个股票数据"""
    try:
        # 尝试不同的字段映射
        code = (item.get('code') or item.get('codes') or 
                item.get('股票代码') or item.get('代码') or 
                item.get('symbol') or item.get('f12') or '')
        
        name = (item.get('name') or item.get('股票名称') or 
                item.get('名称') or item.get('n') or 
                item.get('f14') or '')
        
        price = float(item.get('price') or item.get('最新价') or 
                     item.get('p') or item.get('f2') or 0)
        
        change_rate = float(item.get('change_rate') or item.get('涨跌幅') or 
                           item.get('zdp') or item.get('f3') or 0)
        
        if not code:
            return None
        
        return {
            'code': str(code),
            'name': name,
            'price': price,
            'change_rate': change_rate,
            'fund_inflow': float(item.get('fund_inflow') or item.get('资金流向') or 0),
            'lianban_days': int(item.get('lianban_days') or item.get('连板天数') or 1),
            'is_new_stock': bool(item.get('is_new_stock') or False),
            'first_limit_time': str(item.get('first_limit_time') or ''),
            'last_limit_time': str(item.get('last_limit_time') or ''),
            'limit_type': str(item.get('limit_type') or '连板'),
            'market_value': float(item.get('market_value') or item.get('流通市值') or 0),
            'turnover_rate': float(item.get('turnover_rate') or item.get('换手率') or 0),
            'pe_ratio': float(item.get('pe_ratio') or item.get('市盈率') or 0),
            'is_st': False,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except (ValueError, KeyError) as e:
        logger.warning(f"解析股票数据失败: {e}")
        return None

def _extract_stock_data_from_script(self, script_content, stocks):
    """从JavaScript内容中提取股票数据"""
    try:
        # 尝试查找JSON格式的数据
        json_patterns = [
            r'var\s+\w*data\w*\s*=\s*(\[\s*\{.*?\}\s*\])',
            r'var\s+\w*pool\w*\s*=\s*(\[\s*\{.*?\}\s*\])',
            r'var\s+\w*stocks?\w*\s*=\s*(\[\s*\{.*?\}\s*\])'
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, script_content, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list):
                        parsed_stocks = self._parse_stock_list(data)
                        stocks.extend(parsed_stocks)
                        logger.info(f"从script中提取到{len(parsed_stocks)}只股票")
                except json.JSONDecodeError:
                    continue
                    
    except Exception as e:
        logger.warning(f"从script提取数据失败: {e}")
        
    except requests.RequestException as e:
        logger.error(f"网络请求失败: {e}")
        return []
    except Exception as e:
        logger.error(f"爬取连板股票数据时发生错误: {e}")
        return []

def scrape_lianban_stocks_api():
    """
    通过API接口获取连板股票数据
    """
    try:
        # 直接使用昨日连板板块成分股API
        return scrape_lianban_stocks_list()
        
    except requests.RequestException as e:
        logger.error(f"API请求失败: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        return []
    except Exception as e:
        logger.error(f"API获取连板股票数据失败: {e}")
        return

def scrape_lianban_stocks_list():
    """
    获取昨日连板板块的成分股列表
    """
    try:
        # 东方财富板块成分股API
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        
        params = {
            'pn': 1,  # 页码
            'pz': 100,  # 每页数量
            'po': 1,  # 升序
            'np': 1,  # 
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',  # 固定参数
            'fltt': 2,  # 
            'invt': 2,  # 
            'fid': 'f3',  # 按涨跌幅排序
            'fs': 'b:BK1051',  # 昨日连板板块代码
            'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38,f39,f40,f41,f42,f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65,f66,f67,f68,f69,f70,f71,f72,f73,f74,f75,f76,f77,f78,f79,f80,f81,f82,f83,f84,f85,f86,f87,f88,f89,f90,f91,f92,f93,f94,f95,f96,f97,f98,f99,f100,f101,f102,f103,f104,f105,f106,f107,f108,f109,f110,f111,f112,f113,f114,f115,f116,f117,f118,f119,f120,f121,f122,f123,f124,f125,f126,f127,f128,f129,f130,f131,f132,f133,f134,f135,f136,f137,f138,f139,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f150,f151,f152,f153,f154,f155,f156,f157,f158,f159,f160,f161,f162,f163,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f193,f194,f195,f196,f197,f198,f199,f200'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://data.eastmoney.com/bkzj/BK1051.html',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        # 发送请求
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('rc') != 0:
            logger.info(f"API返回错误码: {data.get('rc')}")
            return []
        
        if not data.get('data') or not data['data'].get('diff'):
            logger.info("API返回数据为空")
            return []
        
        # 提取股票数据
        stocks = []
        stock_list = data['data']['diff']
        
        logger.info(f"API返回{len(stock_list)}只股票数据")
        
        for stock_data in stock_list:
            try:
                stock = {
                    'code': stock_data.get('f12', ''),
                    'name': stock_data.get('f14', ''),
                    'price': float(stock_data.get('f2', 0)),
                    'change_rate': float(stock_data.get('f3', 0)),
                    'change_amount': float(stock_data.get('f4', 0)),
                    'volume': float(stock_data.get('f5', 0)),
                    'amount': float(stock_data.get('f6', 0)),
                    'amplitude': float(stock_data.get('f7', 0)),
                    'turnover_rate': float(stock_data.get('f8', 0)),
                    'pe_ratio': float(stock_data.get('f39', 0)),
                    'pb_ratio': float(stock_data.get('f46', 0)),
                    'market_value': float(stock_data.get('f20', 0)),
                    'fund_inflow': 0,  # 需要其他接口获取
                    'lianban_days': 1,  # 默认1天，需要其他接口获取真实连板天数
                    'is_new_stock': False,
                    'first_limit_time': '',
                    'last_limit_time': '',
                    'limit_type': '连板',
                    'is_st': False,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                stocks.append(stock)
                
            except (ValueError, KeyError) as e:
                logger.warning(f"解析股票数据失败: {e}")
                continue
        
        logger.info(f"成功获取{len(stocks)}只连板股票数据")
        return stocks
        
    except requests.RequestException as e:
        logger.error(f"API请求失败: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        return []
    except Exception as e:
        logger.error(f"API获取连板股票数据失败: {e}")
        return []

def filter_stocks(stocks: List[Dict]) -> List[Dict]:
    """
    过滤掉ST、*ST股票和其他不符合条件的股票
    """
    filtered_stocks = []
    
    for stock in stocks:
        name = stock.get('name', '')
        code = stock.get('code', '')
        
        # 过滤条件
        filters_passed = True
        
        # 1. 过滤ST股票（名称包含ST或*ST）
        if re.search(r'\*?ST', name, re.IGNORECASE):
            logger.info(f"过滤ST股票: {code} {name}")
            filters_passed = False
            continue
            
        # 2. 过滤名称中包含特殊字符的股票
        if re.search(r'[\*\?\!]', name):
            logger.info(f"过滤特殊字符股票: {code} {name}")
            filters_passed = False
            continue
            
        # 3. 过滤价格异常的股票（价格为0或负数）
        if stock.get('price', 0) <= 0:
            logger.info(f"过滤价格异常股票: {code} {name}")
            filters_passed = False
            continue
            
        # 4. 过滤涨跌幅异常的股票（涨跌幅超过20%）
        if abs(stock.get('change_rate', 0)) > 20:
            logger.info(f"过滤涨跌幅异常股票: {code} {name} (涨跌幅: {stock.get('change_rate', 0)}%)")
            filters_passed = False
            continue
            
        # 5. 过滤连板天数为0的股票（确保是真正的连板股）
        if stock.get('lianban_days', 0) <= 0:
            logger.info(f"过滤非连板股票: {code} {name}")
            filters_passed = False
            continue
        
        if filters_passed:
            # 标记为非ST股票
            stock['is_st'] = False
            filtered_stocks.append(stock)
    
    logger.info(f"过滤后剩余{len(filtered_stocks)}只股票")
    return filtered_stocks

def enhance_stock_data(stocks: List[Dict]) -> List[Dict]:
    """
    增强股票数据，添加更多有用的信息
    """
    enhanced_stocks = []
    
    for stock in stocks:
        # 创建增强版本的股票数据
        enhanced_stock = stock.copy()
        
        # 添加计算字段
        enhanced_stock['limit_intensity'] = enhanced_stock['change_rate'] / 10.0  # 涨停强度
        enhanced_stock['fund_efficiency'] = enhanced_stock['fund_inflow'] / enhanced_stock['market_value'] if enhanced_stock['market_value'] > 0 else 0  # 资金效率
        
        # 添加风险等级（基于连板天数和换手率）
        lianban_days = enhanced_stock['lianban_days']
        turnover_rate = enhanced_stock['turnover_rate']
        
        if lianban_days >= 5 and turnover_rate > 20:
            risk_level = "高风险"
        elif lianban_days >= 3 and turnover_rate > 15:
            risk_level = "中高风险"
        elif lianban_days >= 2 and turnover_rate > 10:
            risk_level = "中等风险"
        else:
            risk_level = "低风险"
        
        enhanced_stock['risk_level'] = risk_level
        enhanced_stock['selection_reason'] = f"连板{enhanced_stock['lianban_days']}天，{risk_level}"
        
        enhanced_stocks.append(enhanced_stock)
    
    return enhanced_stocks

def save_to_json(stocks: List[Dict], output_file: str):
    """
    将股票数据保存到JSON文件
    """
    try:
        # 构建输出数据结构
        output_data = {
            "scrape_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_stocks": len(stocks),
            "data_source": "东方财富网连板股票",
            "stocks": stocks
        }
        
        # 确保目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # 保存到JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"成功保存{len(stocks)}只股票数据到 {output_file}")
        
    except Exception as e:
        logger.error(f"保存JSON文件失败: {e}")

def main():
    """
    主函数
    """
    logger.info("连板股票数据爬取程序启动")
    
    # 爬取连板股票数据
    raw_stocks = scrape_lianban_stocks()
    
    if not raw_stocks:
        logger.error("未能获取连板股票数据")
        return
    
    # 过滤ST股票和其他不符合条件的股票
    filtered_stocks = filter_stocks(raw_stocks)
    
    if not filtered_stocks:
        logger.error("过滤后没有符合条件的股票")
        return
    
    # 增强股票数据
    enhanced_stocks = enhance_stock_data(filtered_stocks)
    
    # 保存到JSON文件
    output_file = r"e:\AI\ConceptSectionStockScreening\selected_stocks.json"
    save_to_json(enhanced_stocks, output_file)
    
    # 打印统计信息
    logger.info("=== 连板股票数据统计 ===")
    logger.info(f"总股票数: {len(enhanced_stocks)}")
    
    # 按连板天数分组统计
    lianban_stats = {}
    for stock in enhanced_stocks:
        days = stock['lianban_days']
        if days not in lianban_stats:
            lianban_stats[days] = 0
        lianban_stats[days] += 1
    
    logger.info("连板天数分布:")
    for days, count in sorted(lianban_stats.items()):
        logger.info(f"  连板{days}天: {count}只")
    
    # 按风险等级统计
    risk_stats = {}
    for stock in enhanced_stocks:
        risk = stock['risk_level']
        if risk not in risk_stats:
            risk_stats[risk] = 0
        risk_stats[risk] += 1
    
    logger.info("风险等级分布:")
    for risk, count in risk_stats.items():
        logger.info(f"  {risk}: {count}只")
    
    logger.info("连板股票数据爬取完成")

if __name__ == "__main__":
    main()