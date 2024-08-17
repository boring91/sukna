import json
from datetime import datetime
from typing import Literal

import gspread
import pandas as pd


def ci(column_name):
    """
    Converts an Excel column name (e.g., `A`)
    to an index number.
    :param column_name: the column name to covert to index.
    :return:
    """
    column_name = column_name.upper()
    number = 0
    for char in column_name:
        number = number * 26 + (ord(char) - ord('A') + 1)
    return number - 1


def ic(index):
    """
    Converts a numerical index to Excel column
    name.
    :param index: the index to convert to column.
    :return:
    """
    column_name = ''
    while index >= 0:
        index, remainder = divmod(index, 26)
        column_name = chr(remainder + ord('A')) + column_name
        index -= 1
    return column_name


def ar_en_column_name_dict(direction: Literal['ar_en', 'en_ar']):
    ar = ['الاسم الثلاثي',
          'رقم جوال الوسيط او الوسيطة',
          'نوع الجنس',
          'نوع القبيلة',
          'نوع الزواج',
          'طبيعة العائلة',
          'نوع الجنسية',
          'تاريخ الميلاد',
          'الحالة الاجتماعية',
          'الوظيفة',
          'الطول',
          'الوزن',
          'لون البشرة',
          'درجة الوسامة',
          'درجة التدين',
          'درجة الخلق',
          'درجة الذرابة',
          'الحالة الصحية',
          'التدخين',
          'الحالة المادية',
          'المؤهل الدراسي',
          'العمر',
          'درجة الجمال',
          'نوع الحجاب',
          'المنطقة',
          'نوع السكن',
          'اذا كان هناك أشياء تريد ذكرها عن نفسك',
          'تفاصيل الطول والوزن',
          'أذكر الأشياء التي تود أن تكون موجودة في زوجتك غير ما سبق ذكره، إذا كان هناك شيء أو يكتفى بكلمة (لايوجد)',
          'اسم المدينة التي تسكن فيها',
          'اسم المدينة التي تسكنين فيها',
          'اذا كان هناك أشياء تريدين ذكرها عن نفسك',
          'أذكري الأشياء التي تودين أن تكون موجودة في زوجتك غير ما سبق ذكره ، إذا كان هناك شيء او يكتفى بكلمة (لا يوجد)']

    en = ['full_name',
          'intermediary_number',
          'gender',
          'clan_type',
          'marriage_type',
          'family_nature',
          'nationality_type',
          'date_of_birth',
          'marital_status',
          'job',
          'height',
          'weight',
          'skin_color',
          'attractiveness_level',
          'religiosity_level',
          'morality_level',
          'etiquette_level',
          'health_status',
          'smoking',
          'financial_status',
          'educational_qualification',
          'age',
          'beauty_level',
          'hijab_type',
          'province',
          'accommodation_type',
          'male_description_extra',
          'height_weight_details',
          'male_condition_extra',
          'male_city',
          'female_city',
          'female_description_extra',
          'female_condition_extra']

    zipped = zip(ar, en) if direction == 'ar_en' else zip(en, ar)
    return {k: v for (k, v) in zipped}


def calculate_age(date_of_birth):
    today = datetime.today()
    age = today.year - date_of_birth.year - ((today.month, today.day) < (date_of_birth.month, date_of_birth.day))
    return age


def load_data(file_id: str,
              service_account_json_path: str,
              max_rows: int) -> pd.DataFrame:
    with open(service_account_json_path, 'r') as f:
        sa_json = json.load(f)
    gc = gspread.service_account_from_dict(sa_json)

    wks = gc.open_by_key(file_id).sheet1

    start_range = 'A'
    end_range = 'CU'

    sheet = wks.get(f'{start_range}1:{end_range}{max_rows}')

    df = pd.DataFrame(sheet[1:], columns=sheet[0])

    df = df.rename(columns=ar_en_column_name_dict('ar_en'))

    df = df.map(lambda x: x.replace('غير مهم', 'not_important') if isinstance(x, str) else x)

    df = df.replace('ذكر', 'male')
    df = df.replace('أنثى', 'female')

    df = df.replace('قبيلي', 'with')
    df = df.replace('قبيلية', 'with')
    df = df.replace('غير قبيلي', 'without')
    df = df.replace('غير قبيلية', 'without')

    df = df.replace('معلن', 'ceremony')
    df = df.replace('مسيار', 'without_ceremony')

    return df


def generate_gender_df(df: pd.DataFrame, gender: Literal['male', 'female']):
    dc_ranges = {
        'male': ['D', 'AA', 'AB', 'AX'],
        'female': ['AZ', 'BX', 'BY', 'CT']
    }

    ds = dc_ranges[gender][0]
    de = dc_ranges[gender][1]
    cs = dc_ranges[gender][2]
    ce = dc_ranges[gender][3]

    d_range = range(ci(ds), ci(de) + 1)
    c_range = range(ci(cs), ci(ce) + 1)

    filtered_df = df[df.iloc[:, 2] == gender].reset_index(drop=True)

    base_df = filtered_df.iloc[:, list(d_range) + list(c_range)]

    d_df = pd.concat([filtered_df['intermediary_number'], base_df.iloc[:, :len(d_range)]], axis=1)
    d_df['row_number'] = df[df.iloc[:, 2] == gender].index + 1

    c_df = base_df.iloc[:, len(d_range):]

    return base_df, d_df, c_df


def compute_match(
        md_df: pd.DataFrame,
        mc_df: pd.DataFrame,
        fd_df: pd.DataFrame,
        fc_df: pd.DataFrame,
        m_id: int,
        f_id: int) -> (float, dict, dict):
    # These standards are compared by taking the
    # standard's value from a description dataframe
    # and checking its match in the conditions dataframe
    comparison_standards = [
        'family_nature',
        'nationality_type',
        'marital_status',
        'job',
        'height',
        'weight',
        'skin_color',
        'attractiveness_level',
        'religiosity_level',
        'morality_level',
        'etiquette_level',
        'health_status',
        'smoking',
        'financial_status',
        'educational_qualification',
        'beauty_level',
        'hijab_type',
        'province'
    ]

    # These standards are compared by checking the
    # intersection of the values in the condition
    # dataframes only; i.e., description dataframes
    # will NOT be used in the comparison.
    condition_only_standards = [
        'accommodation_type'
    ]

    all_standards = comparison_standards + condition_only_standards + ['age']

    result_m = dict([(x, -1) for x in all_standards])
    result_f = result_m.copy()

    md = md_df.loc[m_id]
    mc = mc_df.loc[m_id]

    fd = fd_df.loc[f_id]
    fc = fc_df.loc[f_id]

    def is_exact_match(field, c_only=False):
        if c_only:
            return mc[field] == 'not_important' or fc[field] == 'not_important' or mc[field] == fc[field]

        return (mc[field] == 'not_important' or mc[field] == fd[field]) and \
            (fc[field] == 'not_important' or fc[field] == md[field])

    def compute_standard_score(std, c, d):
        if std not in c:
            return -1

        conditions = list(map(lambda x: str(x).strip(), c[std].split(',')))

        if 'not_important' in conditions:
            return 1

        elif str(d[std]).strip() in conditions:
            return 1

        return 0

    def compute_age_score(c, d):
        try:
            c_age = c['age']
            d_dob = d['date_of_birth']
            value = list(map(lambda x: int(x.strip()), c_age.split('-'))) if '-' in str(c_age) else int(c_age)
            date_of_birth = datetime.strptime(d_dob, '%m/%d/%Y')
            age = calculate_age(date_of_birth)

            # in case the value was a single number:
            if type(value) is int:
                return 1 if age == value else 0

            # in case the value was a range:
            elif type(value) is list and len(value) == 2:
                return 1 if value[0] <= age <= value[1] else 0
        except e:
            pass

        return 0

    # Ensure same clan & marriage type
    if not is_exact_match('clan_type') or not is_exact_match('marriage_type', c_only=True):
        return 0

    score = 0
    total_score = 0

    # Get the date of birth/age score:
    result_m['age'] = compute_age_score(mc, fd)
    score += result_m['age']
    total_score += 1

    result_f['age'] = compute_age_score(fc, md)
    score += result_f['age']
    total_score += 1

    for standard in comparison_standards:

        # compute the m->f
        r = compute_standard_score(standard, mc, fd)
        if r > -1:
            result_m[standard] = r
            score += r
            total_score += 1

        # compute the f->m
        r = compute_standard_score(standard, fc, md)
        if r > -1:
            result_f[standard] = r
            score += r
            total_score += 1

    for standard in condition_only_standards:
        r = compute_standard_score(standard, mc, fc)
        if r > -1:
            result_m[standard] = r
            result_f[standard] = r
            score += r
            total_score += 1

    result_m = {k: v for k, v in result_m.items() if v != -1}
    result_f = {k: v for k, v in result_f.items() if v != -1}

    return score / total_score, result_m, result_f


def compute_results(
        md_df: pd.DataFrame,
        mc_df: pd.DataFrame,
        fd_df: pd.DataFrame,
        fc_df: pd.DataFrame,
        min_threshold,
        max_threshold):
    score_results = []

    for m_id in md_df.index:
        for f_id in fd_df.index:
            score, result_m, result_f = compute_match(md_df, mc_df, fd_df, fc_df, m_id, f_id)
            if not (min_threshold <= score <= max_threshold):
                continue

            score_results.append({
                'male': md_df.loc[m_id],
                'male_c': mc_df.loc[m_id],
                'male_r': result_m,
                'female': fd_df.loc[f_id],
                'female_c': fc_df.loc[f_id],
                'female_r': result_f,
                'score': score
            })

    return score_results


def get_result_df() -> pd.DataFrame:
    max_rows_to_load = 1000
    file_id = '15iw-HH19cS_gFArQhAxGmHdDf9Y7m9_nU4NcZIyc_Z8'
    service_account_json_path = 'service_account.json'
    min_score_threshold = 0.6
    max_score_threshold = 1.0

    df = load_data(file_id, service_account_json_path, max_rows_to_load)

    males_df, md_df, mc_df = generate_gender_df(df, 'male')
    females_df, fd_df, fc_df = generate_gender_df(df, 'female')

    results = compute_results(md_df, mc_df, fd_df, fc_df, min_score_threshold, max_score_threshold)

    en_ar = ar_en_column_name_dict('en_ar')

    results_df = pd.DataFrame(map(lambda x: {
        'نسبة التوافق': x['score'] * 100,
        'رقم الصف للرجل': x['male']['row_number'],
        'اسم الرجل': x['male']['full_name'],
        'رقم الوسيط للرجل': x['male']['intermediary_number'],
        'المدينة للرجل': x['male']['male_city'],
        'تفاصيل الطول والوزن للرجل': x['male']['height_weight_details'],
        'معلومات إضافية للرجل': x['male']['male_description_extra'],
        'شروط إضافية للرجل': x['male_c']['male_condition_extra'],
        'رقم الصف للفتاة': x['female']['row_number'],
        'اسم الفتاة': x['female']['full_name'],
        'رقم الوسيط للفتاة': x['female']['intermediary_number'],
        'المدينة للفتاة': x['female']['female_city'],
        'تفاصيل الطول والوزن للفتاة': x['female']['height_weight_details'],
        'معلومات إضافية للفتاة': x['female']['female_description_extra'],
        'شروط إضافية للفتاة': x['female_c']['female_condition_extra'],
        **{en_ar[k] + ' (الرجل)': 'x' if v == 0 else '' for k, v in x['male_r'].items()},
        **{en_ar[k] + ' (الفتاة)': 'x' if v == 0 else '' for k, v in x['female_r'].items()},
    }, results))

    return results_df


def main():
    results_df = get_result_df()
    results_df.to_excel('results.xlsx')
    print(results_df)


if __name__ == '__main__':
    main()
