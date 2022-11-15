

def move(r, dest):
    import os
    from de_course.vars import path

    if dest == 'processed-':
        os.replace(f'{path}/json/ga_hits_new_{r}.json', f'{path}/{dest}/ga_hits_new_{r}.json')
    os.replace(f'{path}/json/ga_sessions_new_{r}.json', f'{path}/{dest}/ga_sessions_new_{r}.json')


def sessions_prepare():
    import pandas as pd
    import re
    import json
    import pickle
    import os
    from de_course.vars import path
    from de_course.vars import engine_de

    sessions_add = pd.DataFrame()
    rez = sorted(os.listdir(f'{path}/json'))
    sql_check_date = pd.read_sql('SELECT visit_date FROM sessions', engine_de)
    count_pr = 0
    count_all = 0

    # пробегаем по папке json
    for item in rez:
        r = re.split('[_.]', item)

        # открываем все файлы sessions
        if r[1] == 'sessions':
            count_all += 1
            with open(f'{path}/json/{item}', 'r') as f:
                s = json.load(f)

            # проверяем наличие данных в json'е
            if len(s[r[3]]) != 0:
                for i in s.values():
                    s_df = pd.DataFrame(i)

                # проверяем таблицу в sql на наличие данных
                # если данные уже присутствуют, то json не обрабатывается
                if sql_check_date[sql_check_date['visit_date'] == r[3]].shape[0] > 0:
                    print(f'WARNING: the data is present in {r[3]}')

                    # все необработанные файлы sessions и hits переносим в папку 'processed-'
                    move(r[3], 'processed-')

                # иначе добавляем данные к датафрейму
                else:
                    sessions_add = pd.concat([sessions_add, s_df])
                    print(f'add_to_df: {item}')
                    count_pr += 1

                    # все обработанные файлы sessions переносим в папку 'processed+'
                    move(r[3], 'processed+')
            else:
                print(f'NO DATA: {r[3]}')
                move(r[3], 'processed-')
        else:
            continue
    print(f'processed files: {count_pr}')
    print(f'warnings: {count_all - count_pr}')

    # если новые данные присутствуют, сохраняем их в pickle
    if count_pr != 0:
        pickle.dump(sessions_add, open(f'{path}/processed+/sessions_pickle.pkl', 'wb'))
        print('pickle file complete')
    else:
        print('no data')


if __name__ == '__main__':
    sessions_prepare()