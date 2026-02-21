import pandas as pd
import numpy as np
import re
from datetime import datetime

input_file = "data.xlsx"
df_raw = pd.read_excel(input_file, engine='openpyxl', header=None)

header_row = df_raw.iloc[0, 0]
headers = [h.strip() for h in header_row.split(',')]

data_rows = []
for i in range(1, len(df_raw)):
    row_value = df_raw.iloc[i, 0]
    if pd.notna(row_value):
        row_data = [item.strip() for item in str(row_value).split(',')]
        while len(row_data) < len(headers):
            row_data.append(None)
        data_rows.append(row_data)

df = pd.DataFrame(data_rows, columns=headers)
df_clean = df.copy()

def normalize_fio(fio):
    if pd.isna(fio):
        return np.nan
    fio = str(fio).strip()
    parts = re.split(r'[.\s]+', fio)
    parts = [p for p in parts if p]
    if len(parts) >= 3:
        surname = parts[0]
        name = parts[1][0] if parts[1] else ''
        patronymic = parts[2][0] if len(parts) > 2 and parts[2] else ''
        return f"{surname} {name}.{patronymic}."
    elif len(parts) == 2:
        surname = parts[0]
        name = parts[1][0] if parts[1] else ''
        return f"{surname} {name}."
    else:
        return fio

df_clean['FIO_normalized'] = df_clean['FIO_owner'].apply(normalize_fio)

def normalize_company(name):
    if pd.isna(name):
        return np.nan
    name = str(name).strip()
    name = name.replace('"', '').replace('«', '').replace('»', '')
    name = re.sub(r'\s+', ' ', name)
    return name

df_clean['Company_normalized'] = df_clean['Company_name'].apply(normalize_company)

df_clean['INN_clean'] = df_clean['INN_company'].astype(str)
df_clean['INN_clean'] = df_clean['INN_clean'].replace('nan', np.nan)
df_clean['INN_clean'] = df_clean['INN_clean'].replace('None', np.nan)

def normalize_ownership(own):
    if pd.isna(own) or own == '':
        return np.nan
    own = str(own).strip().replace(' ', '')
    if '%' in own:
        try:
            return float(own.replace('%', ''))
        except:
            return np.nan
    else:
        try:
            return float(own) * 100
        except:
            return np.nan

df_clean['Ownership_percent'] = df_clean['Ownership'].apply(normalize_ownership)

def normalize_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return np.nan
    date_str = str(date_str).strip()
    formats = ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%Y.%m.%d']
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    return np.nan

df_clean['Date_normalized'] = df_clean['Ownership_date'].apply(normalize_date)

missing_inn = df_clean[df_clean['INN_clean'].isna()]
empty_own = df_clean[df_clean['Ownership_percent'].isna()]

company_groups = df_clean.groupby(['Company_normalized', 'INN_clean'])
companies_over_100 = []
companies_with_changes = []

for (company, inn), group in company_groups:
    total = group['Ownership_percent'].sum()
    if pd.notna(total) and total > 100:
        companies_over_100.append({
            'Компания': company,
            'ИНН': inn,
            'Суммарная_доля_%': round(total, 1)
        })
    if len(group) > 1 and group['Date_normalized'].nunique() > 1:
        companies_with_changes.append({
            'Компания': company,
            'ИНН': inn
        })

owner_companies = df_clean.groupby('FIO_normalized')['Company_normalized'].nunique()
multiple_owners_list = []
for owner, count in owner_companies[owner_companies > 1].items():
    companies = df_clean[df_clean['FIO_normalized'] == owner]['Company_normalized'].unique()
    multiple_owners_list.append({
        'Владелец': owner,
        'Количество_компаний': count
    })

result_df = pd.DataFrame({
    'ФИО_владельца': df_clean['FIO_normalized'],
    'Компания': df_clean['Company_normalized'],
    'ИНН': df_clean['INN_clean'],
    'Доля_%': df_clean['Ownership_percent'],
    'Регион': df_clean['Region'],
    'Источник': df_clean['Source'],
    'Дата': df_clean['Date_normalized']
})

output_file = "result_data.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    result_df.to_excel(writer, sheet_name='Обработанные данные', index=False)
    
    stats_data = {
        'Показатель': [
            'Всего записей',
            'Уникальных компаний',
            'Уникальных владельцев',
            'Записей без ИНН',
            'Записей без доли',
            'Средняя доля, %',
            'Компаний с долей >100%',
            'Компаний с изменениями',
            'Владельцев в нескольких компаниях'
        ],
        'Значение': [
            len(df_clean),
            df_clean['Company_normalized'].nunique(),
            df_clean['FIO_normalized'].nunique(),
            len(missing_inn),
            len(empty_own),
            round(df_clean['Ownership_percent'].mean(), 1) if not df_clean['Ownership_percent'].isna().all() else 0,
            len(companies_over_100),
            len(companies_with_changes),
            len(multiple_owners_list)
        ]
    }
    stats_df = pd.DataFrame(stats_data)
    stats_df.to_excel(writer, sheet_name='Статистика', index=False)
    
    if companies_over_100:
        over_100_df = pd.DataFrame(companies_over_100)
        over_100_df.to_excel(writer, sheet_name='Доли более 100%', index=False)
    
    if companies_with_changes:
        changes_df = pd.DataFrame(companies_with_changes)
        changes_df.to_excel(writer, sheet_name='Изменения долей', index=False)
    
    if multiple_owners_list:
        multiple_df = pd.DataFrame(multiple_owners_list)
        multiple_df.to_excel(writer, sheet_name='Владельцы в нескольких', index=False)

print(f"Результат сохранен в {output_file}")