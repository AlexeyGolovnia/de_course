
def manufacturer(s):
    import re
    import numpy as np

    t = s.split('?')[0]
    t2 = t.split('/')
    try:
        if t2[2] == 'all':
            return t2[3]
        elif t2.count('cars') == 1:
            t0 = t2.index('cars')
            t1 = t2[t0 + 1]
            if re.search('\d+', t1) is None:
                return t1
        else:
            return np.NaN
    except:
        return np.NaN


def model(s):
    import numpy as np

    t = s.split('?')[0]
    t2 = t.split('/')
    try:
        return t2[4]
    except:
        return np.NaN


def hits_processing():
    import pandas as pd
    import os
    import json
    import pickle
    from de_course.vars import path

    rez = os.listdir(f'{path}/json')
    hits_add = pd.DataFrame()
    count_pr = 0

    # пробегаем по всем оставшимся файлам hits
    for item in rez:
        try:
            with open(f'{path}/json/{item}', 'r') as f:
                h = json.load(f)
            for i in h.values():
                h_df = pd.DataFrame(i)

            # добавляем данные к датафрейму и переносим в папку 'processed+'
            hits_add = pd.concat([hits_add, h_df])
            os.replace(f'{path}/json/{item}', f'{path}/processed+/{item}')
            count_pr += 1
        except:
            ()

    print(f'add files: {count_pr}')

    # обрабатываем датафрейм, если данные присутствовали
    if count_pr != 0:
        print('processing')

        # удаляем неинформативные колонки 'hit_type' и 'event_value'
        hits_add = hits_add.drop(['hit_type', 'event_value'], axis=1)

        # создаём новые колонки 'manufacturer' и 'model'
        # данные извлекаем из колонки 'hit_page_path'
        hits_add[['manufacturer', 'model']] = hits_add['hit_page_path'].apply([manufacturer, model])

        # удаляем колонку 'hit_page_path' и преобразуем колонку hit_date в тип datetime
        hits_add = hits_add.drop(['hit_page_path'], axis=1)
        hits_add['hit_date'] = pd.to_datetime(hits_add['hit_date'])
        print('ok')

        # сохраняем обработанные данные в pickle
        pickle.dump(hits_add, open(f'{path}/processed+/hits_pickle.pkl', 'wb'))
        print('pickle file complete')
    else:
        print('no data')


if __name__ == '__main__':
    hits_processing()