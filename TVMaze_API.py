# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 20:58:06 2021

@author: George Sklavounos
"""

import json
import sqlite3
from pandas.io import sql
import datetime as dt

import pandas as pd
from flask import Flask
from flask import request
from flask import send_from_directory
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import reqparse
import os
import math

import matplotlib.pyplot as plt

app = Flask(__name__)
api = Api(app,
          version="1.0",
          default='TV Shows',
          title='TV Shows Dataset',
          description="An API for TV shows; imports from the TVMaze API and stores locally")

import_model = api.model('Import', {
    'name': fields.String})

show_model = api.model('Show', {
    'id': fields.Integer,
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.String,
    'status': fields.String,
    'runtime': fields.Integer,
    'premiered': fields.Date,
    'officialSite': fields.String,
    'schedule': fields.String,
    'rating': fields.String,
    'weight': fields.Integer,
    'network': fields.String,
    'summary': fields.String,
    '_links': fields.String})



@api.route('/tv-shows/statistics', doc={'params': {'format': 'How to return the object, default is JSON',
                                                   'by': 'Values to show, default is language'}})
class ShowsStats(Resource):
    def get(self):
        
        # Get arguments
        stats = reqparse.RequestParser()
        stats.add_argument('format', type=str)
        stats.add_argument('by', type=str)
        
        args = stats.parse_args()
        
        # Set default values and catch issues
        if args['format'] is None:
            args['format'] = 'json'
        if args['by'] is None:
            args['by'] = 'language'
            
        if args['by'] not in ['language','genres','status','type']:
            api.abort(404, f'BY parameter must be either language, genres, status, or type; got {args["by"]}')
            
        if args['format'] not in ['json','image']:
            api.abort(404, f'FORMAT parameter must be either json or image; got {args["format"]}')
        
        # Grab data
        cnx = sqlite3.connect(database_file)
        df = sql.read_sql('SELECT * FROM TV_Shows', cnx)
        
        # We're returning a JSON/image of proportions, so convert totals to %
        def percentify(df, col):
            temp = df.groupby(col)[col].count()
            temp = round((temp / temp.sum()) * 100, 1)
            if args['format'] == 'json':
                temp = temp.to_json()
            return temp
        
        # Language/status/type are the same returns, but genres is special
        if args['by'] in ['language','status','type']:
            to_ret = percentify(df, args['by'])
        elif args['by'] == 'genres':
            
            # Genres are quite broad, and include both the thematic categories a show falls into
            # (e.g., comedy, action, drama), as well as the broader subgenre of overall media
            # (e.g., sci fi, fantasy, medical, crime). So I thought it would be worth visualising
            # what proportion of the former are made up of the latter.
            
            # Get only name/genres, then split list into requisite components
            x = df[['name','genres']]
            new = x.copy()
            new['gen_con'] = new['genres'].apply(json.loads)
            new = new.explode(column='gen_con')
            
            # These are our themes -- split the df based on whether the themes
            # are present or not, then combine the two resulting dataframes
            theme_genres = ['Adventure','Action','Drama','Comedy','Thriller','Horror']
            themes = new.loc[new['gen_con'].isin(theme_genres)]
            cats = new.loc[~new['gen_con'].isin(theme_genres)]
            cats = cats.dropna()
            comb = pd.merge(themes, cats, how='inner', on='name')
            
            # Get only relevant columns and rename, then groupby theme and sub-genre
            # and calculate percentage of each theme which is made up of each sub-genre
            comb = comb[['gen_con_x','gen_con_y']]
            
            comb.rename(columns={'gen_con_x': 'Theme',
                                 'gen_con_y': 'Sub-Genre'},
                        inplace=True)
            
            new_comb = comb.groupby(['Theme','Sub-Genre']).size()
            
            final_comb = new_comb.groupby(level=0).transform(lambda x: (x / x.sum()) * 100)
            
            if args['format'] == 'json':
                to_ret = final_comb.to_json()             
        
        if args['format'] == 'json':
            
            ret = {}
            
            # Get those shows which have been updated in the last 24 hours
            df['day'] = pd.to_datetime(df['last-update'], format="%Y%m%d %H:%M:%S")
            
            new = df.loc[df['day'] >= (dt.datetime.now() - dt.timedelta(hours=24))]
            
            # Populate dict
            ret['total'] = len(df)
            ret['total-updated'] = len(new)
            ret['values'] = json.loads(to_ret)
            
            return ret
        else:
            # Define parameters of chart based on whether it's genres or not
            if args['by'] == 'genres':
                new_comb.unstack().plot(kind='bar', 
                                        stacked=True, 
                                        ylabel='Count',
                                        title='Sub-genre of TV shows by theme',
                                        figsize=(8,8))
            else:
                to_ret.plot(kind='bar', 
                            ylabel='percent', 
                            rot=90,
                            title=f'TV shows by {args["by"]}',
                            figsize=(8,8))
            
            # Save chart as image and send via API
            plt.savefig("Q6.png", bbox_inches='tight')
            plt.close()
            directory = os.getcwd()
            return send_from_directory(directory=directory, filename='Q6.png')

@api.route('/tv-shows', doc={'params': {'order_by': 'Field to order returned objects by; must begin with +/-, default is "id"',
                                         'page': 'Which page to return',
                                         'page_size': 'Size of pages',
                                         'filter': 'Which columns to return'}})
class ShowsList(Resource):
    
    @api.response(200, 'Successful')
    @api.doc(description='Get all filter elements of TV shows by order_by')
    def get(self):
        
        parser = reqparse.RequestParser()
        parser.add_argument('order_by', type=str, help='name of show')
        parser.add_argument('page', type=int)
        parser.add_argument('page_size', type=int)
        parser.add_argument('filter', type=str)
        
        cnx = sqlite3.connect(database_file)
        df = sql.read_sql('SELECT * FROM TV_Shows', cnx)
        
        # Get parameters of query and set default values where necessary
        args = parser.parse_args()
        if args['order_by'] is None:
            args['order_by'] = ' id'
            # args['order_by'] = '-name'        
        if args['page'] is None:
            args['page'] = 1
        if args['page_size'] is None:
            args['page_size'] = 100
        if args['filter'] is None:
            args['filter'] = 'id,name'    
            # args['filter'] = 'genres,rating,network,schedule'
        
        # For order_by, split the string and add the +/- to asc list and
        # the body to list order_by
        orderby_arg = args['order_by'].split(',')
        
        order_by = []
        asc = []
        
        for name in orderby_arg:
            temp = name[1:]
            
            # No +/- at the beginning
            if name[0] != ' ' and name[0] != '-' and name[0] != '+':
                api.abort(404, f'order_by must begin with + or -; {name[0]} found')
            # Wrong page format
            if args['page'] <= 0:
                api.abort(404, f'Page must be positive number; got {args["page"]}')
            # Incorrect order_by
            if temp not in ['id','name','runtime','premiered','rating-average']:
                api.abort(404, f'order_by must be either id,name,runtime,premiered, or rating-average; got {temp}')
            
            # Make id the tvmaze-id
            if temp == 'id':
                order_by.append('tvmaze-id')
            else:
                order_by.append(temp)
            
            # '+' is removed from the URL and replaced with ' ', so we just check for ' '
            if name[0] == ' ' or name[0] == '+':
                asc.append(True)
            else:
                asc.append(False)
        
        # If we're going by rating average, then extract the value in 'rating' column
        if 'rating-average' in order_by:
            def avg_rating(row):
                row = row.split(':')
                ret = row[1][:-1]
                return ret
        
            df['rating-average'] = df['rating'].apply(avg_rating)
        
        # Sort values by order_by and asc, then drop index and create 'id' from 'tvmaze-id'
        df = df.astype({'tvmaze-id': int,
                        'runtime': float})

        df = df.sort_values(by=order_by, ascending=asc)
        
        df.drop(columns=['index'], inplace=True)
        
        df['id'] = df['tvmaze-id']        
        
        # Split filters and check validity
        filt = args['filter'].split(',')
        
        for filt_name in filt:
            if filt_name not in df.columns:
                api.abort(404, f'filter must be in the DB; got {filt_name}')
        
        # Keep only those columns in the df that are listed in filter, then
        # check for the cols that need to be converted to JSON before being
        # returned (i.e., those that are lists or dicts)        
        df = df[filt]
        
        def json_it(row):
            return json.loads(row)
        
        for col in df:
            if col in['genres','schedule','rating','network']:
                df[col] = df[col].apply(json_it)
        
        # Check if the page_size value is larger than the df itself -- if it
        # is then we only need to return one page, otherwise get the appropriate
        # values based on page and page_size, then return along with links.        
        pages = {}
        
        if args['page_size'] >= len(df) and args['page'] == 1:
            pages['page'] = args['page']
            pages['page_size'] = args['page_size']
            pages['tv-shows'] = json.loads(df.to_json(orient='records'))
            pages['_links'] = {'self': {
                                'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"]}&page_size={args["page_size"]}&filter={args["filter"]}'}}
        
        else:
            
            start = args['page'] * args['page_size']
            end = (args['page'] + 1) * args['page_size']
            
            links = {'self': {
                    'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"]}&page_size={args["page_size"]}&filter={args["filter"]}'}}
            
            # If the starting record is already outside the bounds of the df
            if start > len(df):
                api.abort(404, f'Database has a total of {len(df)} records; max pages is {math.ceil(len(df) / args["page_size"])}')
            
            # If we're on the first page and there's more than one page
            if args['page'] == 1 and len(df) > args['page_size']:
                links['next'] = {'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"] + 1}&page_size={args["page_size"]}&filter={args["filter"]}'}
            # If we're on the last page
            elif end >= len(df):
                links['previous'] = {'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"] - 1}&page_size={args["page_size"]}&filter={args["filter"]}'}
            # All other cases
            else:
                links['next'] = {'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"] + 1}&page_size={args["page_size"]}&filter={args["filter"]}'}
                links['previous'] = {'href': f'http://{request.host}/tv-shows?order_by={args["order_by"]}&page={args["page"] - 1}&page_size={args["page_size"]}&filter={args["filter"]}'}
                
            page_x = df[(start-1):(end-1)]
            
            pages['page'] = args['page']
            pages['page_size'] = args['page_size']
            pages['tv-shows'] = json.loads(page_x.to_json(orient='records'))
            pages['_links'] = links
            
        return pages

@api.route('/tv-shows/<int:id>')
@api.param('id', 'The unique identifier for the TV show; same as the tvmaze ID')
class Shows(Resource):
    
    @api.response(200, 'Successful')
    @api.doc(description='Get a specific TV show based on its id')
    def get(self, id):
        cnx = sqlite3.connect(database_file)
        df = sql.read_sql('SELECT * FROM TV_Shows', cnx)
        
        # return df['name']
        
        df = df.astype({'tvmaze-id': int,
                        'runtime': float})
        df.sort_values(inplace=True, by='tvmaze-id', ascending=True, ignore_index=True)
        
        ret = df.loc[df['tvmaze-id'] == id]
        
        if len(ret) == 0:
            api.abort(404, f'Show with id {id} does not exist')
            return
        
        ret = ret[['tvmaze-id',
                    'name',
                    'last-update',
                    'type',
                    'language',
                    'genres',
                    'status',
                    'runtime',
                    'premiered',
                    'officialSite',
                    'schedule',
                    'rating',
                    'weight',
                    'network',
                    'summary']]
        
        current = df.loc[df['tvmaze-id'] == id].index[0]
        
        links = {'self': {'href': f'http://{request.host}/tv-shows/{id}'}}
        
        if current == 0 and len(df) != 1:
            nex = df['tvmaze-id'].iloc[current + 1]
            links['next'] = {'href': f'http://{request.host}/tv-shows/{nex}'}
        elif current == len(df) - 1:
            prev = df['tvmaze-id'].iloc[(current - 1)]
            links['previous'] = {'href': f'http://{request.host}/tv-shows/{prev}'}
        else:
            nex = df['tvmaze-id'].iloc[current + 1]
            links['next'] = {'href': f'http://{request.host}/tv-shows/{nex}'}

            prev = df['tvmaze-id'].iloc[(current - 1)]
            links['previous'] = {'href': f'http://{request.host}/tv-shows/{prev}'}
        
        # return ret.loc[0,'tvmaze-id']
        
        to_ret = {'id': int(ret['tvmaze-id'][ret.index[0]])}
        
        for col in ret.columns:
            if col in ['genres','schedule','rating','network']:
                to_ret[col] = json.loads(ret[col][ret.index[0]])
            elif col in ['tvmaze-id','runtime']:
                try:
                    to_ret[col] = int(ret[col][ret.index[0]])
                except ValueError:
                    to_ret[col] = None
            else:
                to_ret[col] = ret[col][ret.index[0]]
        
        to_ret['_links'] = links
        
        return to_ret
    
    @api.response(200, 'Successfully deleted TV show')
    @api.response(404, 'TV show not found')
    @api.doc(description='Delete a specific TV show based on its id')
    
    def delete(self, id):
        cnx = sqlite3.connect(database_file)
        df = sql.read_sql('SELECT * FROM TV_Shows', cnx)
        
        if len(df.loc[df['tvmaze-id'] == str(id)]) == 0:
            api.abort(404, f'Show with id {id} does not exist')            
        
        df = df.loc[df['tvmaze-id'] != str(id)]
        
        sql.to_sql(df, name='TV_Shows', con=cnx, if_exists='replace', index=False)
        
        return {'message': f'The TV show with id {id} was removed from the database',
                'id': f'{id}'}, 200 
    
    @api.response(200, 'Successfully updated TV show')
    @api.response(404, 'TV show not found')
    @api.response(400, 'Validation error')
    @api.doc(description='Update a specific TV show based on its id')
    @api.expect(show_model)
    def patch(self, id):
        
        cnx = sqlite3.connect(database_file)
        df = sql.read_sql('SELECT * FROM TV_Shows', cnx)
        
        show = df.loc[df['tvmaze-id'] == str(id)]
        
        if len(show) == 0:
            api.abort(404, f'Show with id {id} does not exist')
        
        update = request.json
        
        for name in update.keys():
            if name == 'id' or name == 'tvmaze-id':
                return {'message': 'id and tvmaze-id cannot be changed'}, 400
            elif name == 'genres':
                body = update[name]
                if type(body) is not list:
                    api.abort(404, f'Genres must be a list; got {type(body)}')
            elif name == 'schedule':
                body = update[name]
                if type(body) is not dict:
                    api.abort(404, f'Schedule must be a dict; got {type(body)}')
                for name2 in body.keys():
                    if name2 not in ['time','days']:
                        api.abort(404, 'Schedule must be a dict with time and days fields')
            elif name == 'rating':
                body = update[name]
                if type(body) is not dict:
                    api.abort(404, f'Rating must be a dict; got {type(body)}')
                for name2 in body.keys():
                    if name2 != 'average':
                        api.abort(404, 'Rating must be a dict with average field')
            elif name == 'network':
                body = update[name]
                if type(body) is not dict:
                    api.abort(404, f'Network must be a dict; got {type(body)}')
                for name2 in body.keys():
                    if name2 not in ['id','name','country']:
                        api.abort(404, 'Network must be a dict with id, name and country fields')
        
        # Because tvmaze-id is not the index, we find its index value
        idx = (df.loc[df['tvmaze-id'] == str(id)]).index[0]
        
        for key in update:
            if key not in show_model.keys():
                return {'message': f'Property {key} is invalid'}, 400
            
            if key in ['genres','schedule','rating','network']:
                dumped = json.dumps(update[key])
                df.loc[idx,key] = dumped
            else:
                df.loc[idx,key] = update[key]
        
        updated = str(dt.datetime.now() - dt.timedelta(microseconds=dt.datetime.now().microsecond))
        
        df.loc[idx,'last-update'] = updated
        
        sql.to_sql(df, name='TV_Shows', con=cnx, if_exists='replace', index=False)
        
        to_ret = {'id': id,
                  'last-update': updated,
                  '_links': {
                      'self': {
                          'href': f'http://{request.host}/tv-shows/{id}'}}}
        
        return to_ret

importer = reqparse.RequestParser()
importer.add_argument('name', type=str, required=True)

@api.route('/tv-shows/import', 
           doc={'params': {'name': 'Name of the show to be imported; must be an exact match, and will import all exact matches.'}})
# @api.route('/tv-shows/import')
class ShowsImport(Resource):
    
    @api.response(201, 'TV Show Created')
    @api.response(400, 'Validation error')
    @api.doc(description='Add a tv show to the database by importing from TVMaze API',
             example='Scrubs')
    # @api.expect(import_model, validate=True)
    def post(self):
        
        # Get name fields from query parameters
        name = importer.parse_args()
              
        if 'name' not in name:
            api.abort(404, 'POST requires a name key')
        
        name = name['name']

        if ' ' in name:
            temp = name.split()
            new_name = ''
            for word in temp:
                new_name += word + '%20'
        else:
            new_name = name
            
        # Read JSON data into a dataframe and convert dict column 'show' to columns
        df = pd.read_json(f'http://api.tvmaze.com/search/shows?q={new_name}', orient='records')
        
        df = pd.concat([df.drop(['show'], axis=1), df['show'].apply(pd.Series)], axis=1)
        
        # Check if the exact match of the show exists in the extracted data
        df = df.loc[df['name'].str.lower() == name.lower()]
        
        # In case show doesn't exist
        if len(df) == 0:
            api.abort(404, f'Show {name} does not exist.')
        
        # Set 'tvmaze-id' as show id (since it's already set by tvmaze-id)        
        df.rename(columns={'id': 'tvmaze-id'}, inplace=True)
        
        # Drop columns and set the remainder in the correct order for DB storage
        df = df[['tvmaze-id','name','type','language',
                 'genres','status','runtime','premiered',
                 'officialSite','schedule','rating','weight','network',
                 'summary']]
        
        # Some columns have an invalid type for DB storage (list and dict), so
        # we convert them to strings (and when we extract them they're reverted
        # with 'json.loads()'
        df['genres'] = json.dumps(df['genres'][0])
        df['schedule'] = json.dumps(df['schedule'][0])
        df['rating'] = json.dumps(df['rating'][0])
        df['network'] = json.dumps(df['network'][0])
        
        # Date and time added
        added = str(dt.datetime.now() - dt.timedelta(microseconds=dt.datetime.now().microsecond))
        
        df['last-update'] = added
        
        # Check if show already exists in DB -- if so, exit
        cnx = sqlite3.connect('storage.db')
        check_dup = sql.read_sql(f'select * from TV_Shows where "tvmaze-id" = "{df["tvmaze-id"][0]}"', cnx)
        
        if len(check_dup) == 0:
            sql.to_sql(df, name='TV_Shows', con=cnx, if_exists='append', index=False)
        else:
            api.abort(404, f'Show {name} already exists in the database.')
        
        # Format our returning dict and return the object
        tv_id = int(df['tvmaze-id'][0])
        
        to_ret = {'id': tv_id,
                  'last-update': added,
                  'tvmaze-id': tv_id,
                  '_links': {'self': {'href': f'http://{request.host}/tv-shows/{tv_id}'}}}
        
        return to_ret, 201


if __name__ == '__main__':
    table_name = 'TV_Shows'
    database_file = 'storage.db'
    
    # Check current directory for DB -- create if it doesn't exist
    if database_file not in os.listdir():    
        schema = pd.DataFrame(columns=['tvmaze-id','name','type','language',
                                       'genres','status','runtime','premiered',
                                       'officialSite','schedule','rating','weight','network',
                                       'summary','last-update'])
        
        cnx = sqlite3.connect(database_file)
        sql.to_sql(schema, name=table_name, con=cnx)

    # NOTE: host and port are both hard-coded, as without hard-coding this
    # caused an error on CSE machines
    app.run(host='127.0.0.1', port=5000, debug=True)

    