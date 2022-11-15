
def sessions_processing():
    import pickle
    import pandas as pd
    from de_course.vars import path
    from de_course.vars import engine_de

    # пытаемся открыть pickle, созданный на предыдущем шаге
    try:
        sessions_new = pickle.load(open(f'{path}/processed+/sessions_pickle.pkl', 'rb'))
    except:
        print('no data')
        return

    # преобразуем колонку visit_date в тип datetime
    print('processing')
    sessions_new['visit_date'] = pd.to_datetime(sessions_new['visit_date'])
    print('ok')

    # добавляем данные в sql
    print('add to sql')
    sessions_new.to_sql('sessions', con=engine_de, index=False, chunksize=100_000, if_exists='append')
    print(f'ok. add rows: {sessions_new.shape[0]}')

    # забираем информацию по s_id из sql
    print('obtain from sql')
    from_sql = pd.read_sql('SELECT session_id, s_id FROM sessions', engine_de)
    print('ok')

    # добавляем s_id к датафрейму
    print('add s_id')
    processed = sessions_new[['session_id']].merge(from_sql, how='inner', on='session_id')
    print('ok')

    # пересохраняем pickle с новой информацией
    print('create pickle')
    pickle.dump(processed, open(f'{path}/processed+/sessions_pickle.pkl', 'wb'))
    print('ok')


if __name__ == '__main__':
    sessions_processing()