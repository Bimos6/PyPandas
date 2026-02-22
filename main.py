import pandas as pd
import numpy as np
import re
from datetime import datetime


def read_data(file_path):
    df_raw = pd.read_excel(file_path, engine='openpyxl', header=None)
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
    
    return pd.DataFrame(data_rows, columns=headers)


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
    return fio


def normalize_company(name):
    if pd.isna(name):
        return np.nan
    name = str(name).strip()
    name = name.replace('"', '').replace('«', '').replace('»', '')
    return re.sub(r'\s+', ' ', name)


def normalize_ownership(own):
    if pd.isna(own) or own == '':
        return np.nan
    own = str(own).strip().replace(' ', '')
    try:
        if '%' in own:
            return float(own.replace('%', ''))
        return float(own) * 100
    except:
        return np.nan


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


def clean_data(df):
    df_clean = df.copy()
    
    df_clean['FIO_normalized'] = df_clean['FIO_owner'].apply(normalize_fio)
    df_clean['Company_normalized'] = df_clean['Company_name'].apply(normalize_company)
    
    df_clean['INN_clean'] = df_clean['INN_company'].astype(str)
    df_clean['INN_clean'] = df_clean['INN_clean'].replace(['nan', 'None'], np.nan)
    
    df_clean['Ownership_percent'] = df_clean['Ownership'].apply(normalize_ownership)
    df_clean['Date_normalized'] = df_clean['Ownership_date'].apply(normalize_date)
    
    return df_clean


def analyze_data(df):
    missing_inn = df[df['INN_clean'].isna()]
    empty_own = df[df['Ownership_percent'].isna()]
    
    company_groups = df.groupby(['Company_normalized', 'INN_clean'])
    
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
    
    owner_companies = df.groupby('FIO_normalized')['Company_normalized'].nunique()
    multiple_owners = []
    for owner, count in owner_companies[owner_companies > 1].items():
        multiple_owners.append({
            'Владелец': owner,
            'Количество_компаний': count
        })
    
    return {
        'missing_inn': missing_inn,
        'empty_own': empty_own,
        'companies_over_100': companies_over_100,
        'companies_with_changes': companies_with_changes,
        'multiple_owners': multiple_owners
    }


def create_result_df(df):
    return pd.DataFrame({
        'ФИО_владельца': df['FIO_normalized'],
        'Компания': df['Company_normalized'],
        'ИНН': df['INN_clean'],
        'Доля_%': df['Ownership_percent'],
        'Регион': df['Region'],
        'Источник': df['Source'],
        'Дата': df['Date_normalized']
    }).sort_values(['Компания', 'ФИО_владельца', 'Дата'])


def save_results(df, analysis, output_file):
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        create_result_df(df).to_excel(writer, sheet_name='Обработанные данные', index=False)
        
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
                len(df),
                df['Company_normalized'].nunique(),
                df['FIO_normalized'].nunique(),
                len(analysis['missing_inn']),
                len(analysis['empty_own']),
                round(df['Ownership_percent'].mean(), 1) if not df['Ownership_percent'].isna().all() else 0,
                len(analysis['companies_over_100']),
                len(analysis['companies_with_changes']),
                len(analysis['multiple_owners'])
            ]
        }
        pd.DataFrame(stats_data).to_excel(writer, sheet_name='Статистика', index=False)
        
        if analysis['companies_over_100']:
            pd.DataFrame(analysis['companies_over_100']).to_excel(
                writer, sheet_name='Доли более 100%', index=False)
        
        if analysis['companies_with_changes']:
            pd.DataFrame(analysis['companies_with_changes']).to_excel(
                writer, sheet_name='Изменения долей', index=False)
        
        if analysis['multiple_owners']:
            pd.DataFrame(analysis['multiple_owners']).to_excel(
                writer, sheet_name='Владельцы в нескольких', index=False)


def main():
    input_file = "data.xlsx"
    output_file = "result_data.xlsx"
    
    df = read_data(input_file)
    df_clean = clean_data(df)
    analysis = analyze_data(df_clean)
    save_results(df_clean, analysis, output_file)
    
    print(f"Результат сохранен в {output_file}")


if __name__ == "__main__":
    main()