
def add_to_sql():
    import pickle
    import os
    from de_course.vars import path
    from de_course.vars import engine_de

    # проверяем наличие pickle файлов с данными sessions и hits
    try:
        sessions = pickle.load(open(f'{path}/processed+/sessions_pickle.pkl', 'rb'))
    except:
        print('no sessions data')
        return
    try:
        hits = pickle.load(open(f'{path}/processed+/hits_pickle.pkl', 'rb'))
    except:
        print('no hits data')
        return

    # добавляем в датасет hits колонку s_id, связывающую его с таблицей sessions
    print('join tables')
    hits = hits.join(sessions[['session_id', 's_id']].set_index('session_id'), on='session_id')
    print('ok')

    # удаляем строки в датасете hits не связанные с таблицей sessions
    hits_f = hits[~hits['s_id'].isna()]
    print(f'deleted rows: {hits.shape[0] - hits_f.shape[0]}')

    # добавляем данные в sql
    print('add to sql')
    hits_f.to_sql('hits', con=engine_de, index=False, chunksize=100_000, if_exists='append')
    print(f'ok. add rows: {hits_f.shape[0]}')

    # удаляем файлы pickle
    print('deleting pickles')
    os.remove(f'{path}/processed+/hits_pickle.pkl')
    os.remove(f'{path}/processed+/sessions_pickle.pkl')
    print('ok')


if __name__ == '__main__':
    add_to_sql()