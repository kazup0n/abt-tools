# -*- coding: utf-8 -*-
import luigi
import mysql.connector
import logging
from jinja2 import Environment, FileSystemLoader

import settings

logger = logging.getLogger('luigi-interface')

class CampaignNotFoundException(Exception):
    pass


class CampaignRepository:

   def __init__(self, query_executor):
       self.query_executor = query_executor

   def findByName(self, name):
       result = self.query_executor.fetchall("SELECT name, start_dt, end_dt from campaigns WHERE name=%s", (name,))
       if len(result) == 0:
           raise CampaignNotFoundException('campaign {} not found.'.format(name))
       return result[0]

class QueryExecutor:

  def __init__(self, host, user, password, db):
      self.host = host
      self.user = user
      self.password = password
      self.db = db

  def fetchall(self, query, params):
      connect = None
      stmt = None
      try:
          connect = mysql.connector.connect(host=self.host, db=self.db, user=self.user, passwd=self.password, connection_timeout=30)
          stmt = connect.cursor(buffered=False, dictionary=True)
          stmt.execute(query, params)
          return stmt.fetchall()
      finally:
          if stmt:
              stmt.close
              if connect:
                  connect.close

class SQLFileTemplate:

  def __init__(self, template_dir, template_name):
      self.template_dir = template_dir
      self.template_name = template_name

  def compile(self, params):
       env = Environment(loader=FileSystemLoader(self.template_dir, encoding='utf8'))
       return env.get_template(self.template_name).render(params).encode('utf8')

class CreateSQLForCampaignTask(luigi.Task):

    campaign_name = luigi.Parameter()
    host = luigi.Parameter(settings.host)
    user = luigi.Parameter(settings.user)
    password = luigi.Parameter(settings.password)
    db = luigi.Parameter(settings.database)
    template_dir = luigi.Parameter(settings.template_dir)
    template_name = luigi.Parameter(settings.template_name)
    print_sql = luigi.BooleanParameter(False)

    def run(self):
        # find campaign
        executor = QueryExecutor(host=self.host, user=self.user, password=self.password, db=self.db)
        repository = CampaignRepository(executor)
        campaign = repository.findByName(self.campaign_name)

        params = dict(campaign, additional_vars_to_template = 'value')
        # create sql file from template with campaign
        outfile = luigi.LocalTarget('sql/{campaign_name}'.format(campaign_name=self.campaign_name))
        with outfile.open('w') as file:
            compiled = SQLFileTemplate(self.template_dir, self.template_name).compile(params)
            file.write(compiled)
            if self.print_sql:
                logger.info('**********************************************************************')
                logger.info(compiled)
                logger.info('**********************************************************************')

if __name__ == '__main__':
    luigi.run()
