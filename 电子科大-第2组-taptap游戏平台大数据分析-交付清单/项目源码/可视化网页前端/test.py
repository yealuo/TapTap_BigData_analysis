from flask import Flask
from flask import render_template
import pandas as pd


app = Flask(__name__, template_folder='templates')

@app.route('/')
def test():
    return render_template('test.html')

@app.route('/show.html')
def show():
    hot_android = pd.read_csv('csv/hot/hot_android.csv')
    hot_ios = pd.read_csv('csv/hot/hot_ios.csv')
    hotplay_android = pd.read_csv('csv/hot_play/hotplay_android.csv')
    book_android = pd.read_csv('csv/reservation/book_android.csv')
    book_ios = pd.read_csv('csv/reservation/book_ios.csv')
    return render_template('show.html', hot_android=hot_android, hot_ios=hot_ios, hotplay_android=hotplay_android, book_android=book_android, book_ios=book_ios)

@app.route('/hot_analysis.html')
def hot_analysis():
    hot_android_tag = pd.read_csv('csv/hot/hot_android_tag_count.csv')
    hot_android_tag_json = hot_android_tag.to_json(orient='records')
    hot_ios_tag = pd.read_csv('csv/hot/hot_ios_tag_count.csv')
    hot_ios_tag_json = hot_ios_tag.to_json(orient='records')
    hotplay_android_tag = pd.read_csv('csv/hot_play/hotplay_android_tag_count.csv')
    hotplay_android_tag_json = hotplay_android_tag.to_json(orient='records')
    book_android_tag = pd.read_csv('csv/reservation/book_android_tag_count.csv')
    book_android_tag_json = book_android_tag.to_json(orient='records')
    book_ios_tag = pd.read_csv('csv/reservation/book_ios_tag_count.csv')
    book_ios_tag_json = book_ios_tag.to_json(orient='records')
    return render_template('hot_analysis.html', hot_android_tag_json=hot_android_tag_json, hot_ios_tag_json=hot_ios_tag_json, hotplay_android_tag_json=hotplay_android_tag_json, book_android_tag_json=book_android_tag_json, book_ios_tag_json=book_ios_tag_json)

@app.route('/tagnum.html')
def tagnum1():
    tag_num1 = pd.read_csv('csv/hot/hot_android_tag_num.csv')
    tag_num1['account'] = tag_num1['account'].str.rstrip('%').astype('float')
    tag_num1_pie = [{'name': label, 'value': value} for label, value in zip(tag_num1['number_of_tags_a_game_has'], tag_num1['account'])]
    tag_num11 = pd.read_csv('csv/hot/hot_ios_tag_num.csv')
    tag_num11['account'] = tag_num11['account'].str.rstrip('%').astype('float')
    tag_num11_pie = [{'name': label, 'value': value} for label, value in
                    zip(tag_num11['number_of_tags_a_game_has'], tag_num11['account'])]
    tag_num2 = pd.read_csv('csv/hot_play/hotplay_android_tag_num.csv')
    tag_num2['account'] = tag_num2['account'].str.rstrip('%').astype('float')
    tag_num2_pie = [{'name': label, 'value': value} for label, value in
                    zip(tag_num2['number_of_tags_a_game_has'], tag_num2['account'])]
    tag_num3 = pd.read_csv('csv/reservation/book_android_tag_num.csv')
    tag_num3['account'] = tag_num3['account'].str.rstrip('%').astype('float')
    tag_num3_pie = [{'name': label, 'value': value} for label, value in
                    zip(tag_num3['number_of_tags_a_game_has'], tag_num3['account'])]
    tag_num31 = pd.read_csv('csv/reservation/book_ios_tag_num.csv')
    tag_num31['account'] = tag_num31['account'].str.rstrip('%').astype('float')
    tag_num31_pie = [{'name': label, 'value': value} for label, value in
                     zip(tag_num31['number_of_tags_a_game_has'], tag_num31['account'])]
    return render_template('tagnum.html', tag_num1_pie=tag_num1_pie, tag_num11_pie=tag_num11_pie, tag_num2_pie=tag_num2_pie, tag_num3_pie=tag_num3_pie, tag_num31_pie=tag_num31_pie)

@app.route('/every_tag.html')
def every_tag1():
    hot_android_game = pd.read_csv('csv/hot/hot_android_game_in_tag.csv')
    hot_ios_game = pd.read_csv('csv/hot/hot_ios_game_in_tag.csv')
    hotplay_android_game = pd.read_csv('csv/hot_play/hotplay_android_game_in_tag.csv')
    book_android_game = pd.read_csv('csv/reservation/book_android_game_in_tag.csv')
    book_ios_game = pd.read_csv('csv/reservation/book_ios_game_in_tag.csv')
    return render_template('every_tag.html', hot_android_game=hot_android_game, hot_ios_game=hot_ios_game, hotplay_android_game=hotplay_android_game, book_android_game=book_android_game, book_ios_game=book_ios_game)

@app.route('/high_score_tag.html')
def high_score_tag1():
    hot_android_sorted_tag = pd.read_csv('csv/hot/hot_android_sorted_tag_count.csv')
    hot_android_sorted_tag_json = hot_android_sorted_tag.to_json(orient='records')
    hot_ios_sorted_tag = pd.read_csv('csv/hot/hot_ios_sorted_tag_count.csv')
    hot_ios_sorted_tag_json = hot_ios_sorted_tag.to_json(orient='records')
    hotplay_android_sorted_tag = pd.read_csv('csv/hot_play/hotplay_android_sorted_tag_count.csv')
    hotplay_android_sorted_tag_json = hotplay_android_sorted_tag.to_json(orient='records')
    book_android_sorted_tag = pd.read_csv('csv/reservation/book_android_sorted_tag_count.csv')
    book_android_sorted_tag_json = book_android_sorted_tag.to_json(orient='records')
    book_ios_sorted_tag = pd.read_csv('csv/reservation/book_ios_sorted_tag_count.csv')
    book_ios_sorted_tag_json = book_ios_sorted_tag.to_json(orient='records')
    return render_template('high_score_tag.html', hot_android_sorted_tag_json=hot_android_sorted_tag_json, hot_ios_sorted_tag_json=hot_ios_sorted_tag_json, hotplay_android_sorted_tag_json=hotplay_android_sorted_tag_json, book_android_sorted_tag_json=book_android_sorted_tag_json,
                           book_ios_sorted_tag_json=book_ios_sorted_tag_json)

@app.route('/Comprehensive_ranking.html')
def comprehensive_ranking1():
    hot_android_comprehensive_ranking = pd.read_csv('csv/hot/hot_android_comprehensive_ranking.csv')
    hot_ios_comprehensive_ranking = pd.read_csv('csv/hot/hot_ios_comprehensive_ranking.csv')
    hotplay_android_comprehensive_ranking = pd.read_csv('csv/hot_play/hotplay_android_comprehensive_ranking.csv')
    book_android_comprehensive_ranking = pd.read_csv('csv/reservation/book_android_comprehensive_ranking.csv')
    book_ios_comprehensive_ranking = pd.read_csv('csv/reservation/book_ios_comprehensive_ranking.csv')
    return render_template('Comprehensive_ranking.html', hot_android_comprehensive_ranking=hot_android_comprehensive_ranking, hot_ios_comprehensive_ranking=hot_ios_comprehensive_ranking, hotplay_android_comprehensive_ranking=hotplay_android_comprehensive_ranking, book_android_comprehensive_ranking=book_android_comprehensive_ranking,
                           book_ios_comprehensive_ranking=book_ios_comprehensive_ranking)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
#host='0.0.0.0'可在其他电脑访问，同一个局域网下。
