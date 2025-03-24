import csv
import requests
import re
import os
from time import sleep
from bs4 import BeautifulSoup
from collections import defaultdict

# ================== 配置区 ==================
base_url = "https://www.dongchedi.com/auto/params-carIds-x-{id}"
input_csv = "car_rank_total.csv"
output_dir = "car_data"
retry_times = 3
request_timeout = 10

# 能源类型映射
ENERGY_TYPE_MAP = {
    "纯电": ["electric_consumption", "battery_capacity", "cltc_recharge_mileage"],
    "汽油": ["engine_max_horsepower", "fuel_consumption", "displacement"],
    "油电混合": ["engine_max_horsepower", "battery_capacity", "electric_consumption"],
    "插电式": ["engine_max_horsepower", "battery_capacity", "electric_consumption"],
    "增程式": ["range_extender_type", "battery_capacity", "electric_consumption"]
}

ENERGY_TYPES = {
    "纯电": "electric",
    "汽油": "fuel",
    "油电混合": "hybrid",
    "插电式": "plug-in",
    "增程式": "range-extender",
    "未知": "unknown"
}

# ============================================
def get_unique_ids():
    unique_ids = set()
    with open(input_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                unique_ids.add(int(row['id']))
            except (KeyError, ValueError):
                pass

    if os.path.exists(output_dir):
        for fname in os.listdir(output_dir):
            if fname.endswith('.csv'):
                with open(os.path.join(output_dir, fname), 'r', encoding='utf-8') as f:
                    done_ids = {int(row['ID']) for row in csv.DictReader(f) if row['ID'].isdigit()}
                    unique_ids -= done_ids
    return sorted(unique_ids)

def get_energy_specific_fields(energy_type):
    return ENERGY_TYPE_MAP.get(energy_type, [])

def parse_models_config(soup):
    models = []
    try:
        header = soup.find('div', class_='table_head__FNAvn')
        if not header:
            return models

        model_cols = header.select('div.table_is-head-col__1sAQG:not(:first-child)')
        if not model_cols:
            return models

        models = [{"型号": "未知车型", "价格": "N/A"} for _ in model_cols]

        for idx, col in enumerate(model_cols):
            name_tag = col.select_one('a.cell_car__28WzZ, div.cell_car__28WzZ')
            if name_tag:
                models[idx]["型号"] = re.sub(r'[\ue600-\ue6ff●○※]', '', name_tag.get_text(strip=True))

        price_row = soup.find('div', class_='cell_official-price__1O2th')
        if price_row:
            price_cells = price_row.find_parent('div', class_='table_row__yVX1h').select(
                'div.cell_official-price__1O2th')
            for idx, cell in enumerate(price_cells[:len(models)]):
                models[idx]["价格"] = re.sub(r'[^\d\.万]', '', cell.get_text(strip=True)) + '万'

        config_sections = soup.select('div.table_root__14vH_:not(:first-child)')
        for section in config_sections:
            for row in section.select('div.table_row__yVX1h[data-row-anchor]'):
                anchor = row['data-row-anchor']
                label = row.select_one('.cell_label__ZtXlw').get_text(strip=True)
                cells = row.select('div.cell_normal__37nRi')

                for idx, cell in enumerate(cells[:len(models)]):
                    text = re.sub(r'[\ue600-\ue6ff●○※]', '', cell.get_text(' ', strip=True))
                    models[idx][label] = text

        for model in models:
            energy_type = model.get("能源类型", "N/A")
            specific_fields = get_energy_specific_fields(energy_type)

            for field in specific_fields:
                model[field] = model.get(field, "N/A")

    except Exception as e:
        print(f"解析异常: {str(e)}")
        return []

    return models

def fetch_data(id):
    url = base_url.format(id=id)
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'referer': base_url.format(id=''),
        'accept-language': 'zh-CN,zh;q=0.9'
    }

    for attempt in range(retry_times):
        try:
            response = requests.get(url, headers=headers, timeout=request_timeout)
            if not response.ok or "参数配置" not in response.text:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            results = parse_models_config(soup)

            for model in results:
                model['ID'] = str(id)  # 添加 ID 字段

            if len(results) > 0 and any(res["型号"] != "未知车型" for res in results):
                return id, results

        except Exception as e:
            print(f"ID {id} 第{attempt + 1}次请求异常：{str(e)}")
        sleep(0.5 ** attempt)

    return id, None

def main():
    os.makedirs(output_dir, exist_ok=True)
    ids = get_unique_ids()
    if not ids:
        print("没有需要处理的新ID")
        return

    all_data = defaultdict(list)
    field_set = set()

    print("正在收集字段信息...")
    for idx, id in enumerate(ids, 1):
        print(f"[{idx}/{len(ids)}] 扫描ID: {id}")
        _, results = fetch_data(id)
        if results:
            for model in results:
                field_set.update(model.keys())
                energy = model.get("能源类型", "未知")
                all_data[energy].append(model)

    base_fields = ["ID", "型号", "价格", "能源类型"]
    other_fields = sorted([f for f in field_set if f not in base_fields])
    all_fields = base_fields + other_fields

    print("\n开始写入数据...")
    for energy_type, data in all_data.items():
        file_tag = ENERGY_TYPES.get(energy_type, "unknown")
        output_path = os.path.join(output_dir, f"car_data_{file_tag}.csv")

        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)

        print(f"✅ {energy_type} 类型已写入 {len(data)} 条数据到 {output_path}")

if __name__ == "__main__":
    main()
