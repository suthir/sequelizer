#!/usr/bin/env python
from datetime import datetime
from os.path import join, abspath, dirname, splitext, basename, exists
from multiprocessing import Process
from os import makedirs, listdir, chdir
from time import time
import json
import sys
import glob
import shutil
import subprocess
import logging

app_root = lambda *dirs: join(dirname(abspath(dirname(__file__))), *dirs)

logger = None
DEFAULT_CONFIG_FILE = "conf/config.json"
DEFAULT_DB_DIR = "orms"
DEFAULT_DATA_DIR = "cloud"
DEFAULT_LOGS_DIR = "logs"

__author__    = "Suthir Perumal Kannan"
__email__     = "ksuthirperumal@gmail.com"
__copyright__ = "Copyright (C) Reserved"
__license__   = "GNU General Public License"
__version__   = "1.0"

banner = """
  ____                        _ _
 / ___|  ___  __ _ _   _  ___| (_)_______ _ __ 
 \___ \ / _ \/ _` | | | |/ _ \ | |_  / _ \ '__|
  ___) |  __/ (_| | |_| |  __/ | |/ /  __/ |   
 |____/ \___|\__, |\__,_|\___|_|_/___\___|_|  %s
                |_|                           

""" % (__version__)

class Utils(object):

    def assure_path_exists(self, path):
        try:
            if not exists(path):
                makedirs(path)
        except OSError:
            logger.error("Error occurred while creating the directory {}" % path)

    def execute(self, command, args=[]):
        status = True
        logger.info("Running %s" % command)
        proc = subprocess.Popen(command, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, shell=True)
        for arg in args:
            proc.stdin.write("%s\n" % arg)
            proc.stdin.flush()
        (result, error) = proc.communicate()
        proc.wait()
        if result:
            status = True
            logger.info("Result  %s" % result)
        if error:
            status = False
            logger.error("%s" % error)
        return status


class DB(object):

    def __init__(self, config):
        self.config = config
        self.hostname = config['hostname']
        self.username = config['username']
        self.password = config['password']
        self.database = config['database']

    def __str__(self):
        return("database=%s user=%s password=********"
               % (self.database, self.username))


class Sequelizer(Utils):

    def __init__(self, config_path):
        self.config_path = config_path
        logger.info("Reading sequelizer configuration from %s" % self.config_path)
        with open(self.config_path) as sequelizer_config:
            self.config = json.load(sequelizer_config)
        self.sequelizer = self.config['sequelizer']
        self.cluster_name = self.sequelizer['cluster_name']
        self.orms_path_pattern = self.sequelizer['orms_path_pattern']
        self.enable_auto_purge = self.sequelizer['enable_auto_purge']
        self.max_num_of_orms_to_retain = self.sequelizer['max_num_of_dataset_to_retain']
        self.enable_upload = self.sequelizer['enable_upload']
        self.orms = glob.glob(self.orms_path_pattern)
        self.dataset_dir = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        self.dataset_root_dir = app_root(self.sequelizer['dataset_dir'])
        self.dataset_path = "%s/%s" % (self.sequelizer['dataset_dir'], self.dataset_dir)
        self.assure_path_exists(self.dataset_path)
        self.db = DB(self.config['db'])

    @property
    def cluster(self):
        return self.cluster_name

    def upsert_to_sfdc(self):
        platform_datas = glob.glob(app_root(self.dataset_path) + "/*.csv")
        sfdc_jobs = []
        for platform_data in platform_datas:
            update_platform_data = "java -cp \"%s/*\" -Dsalesforce.config.dir=\"%s\" com.salesforce.dataloader.process.ProcessRunner process.mappingFile=\"%s\" process.name=cloud" % \
                                    (app_root('bin'), app_root('conf'), platform_data)
            sfdc_proc = Process(target=self.execute, args=(update_platform_data,))
            sfdc_jobs.append(sfdc_proc)
            sfdc_proc.start()
        logger.info("Uploading collected datas dated [%s] to salesforce" % (self.dataset_dir))
        for sfdc_job in sfdc_jobs:
            sfdc_job.join()
        logger.info("MICloud data from [%s] have been successfully upserted to salesforce" % self.cluster)

    def collect(self):
        jobs = []
        for orm in self.orms:
            sql_file, sql_ext = splitext(basename(orm))
            data_file = app_root("%s/%s_%s.csv" % (self.dataset_path, self.cluster_name, sql_file))
            if exists(orm):
                self.assure_path_exists(dirname(data_file))
                if self.db.username is not None and self.db.hostname is not None:
                    sql_args = "\"dbname='%s' host='%s' user='%s' password='%s'\" -A -F',' -P'footer=off'" % \
                               (self.db.database, self.db.hostname, self.db.username, self.db.password)
                    sql_cmd = "psql %s -f \"%s\" -v p_Cluster=%s -o \"%s\"" % (sql_args, app_root(orm), self.cluster_name.upper(), data_file)
                    logger.info("Analysing %s from [%s] database on [%s]" % (sql_file, self.db.database, self.cluster_name.upper()))
                    proc = Process(target=self.execute, args=(sql_cmd,))
                    jobs.append(proc)
                    proc.start()

        for job in jobs:
            job.join()

    def purge_historical_datas(self):
        all_datas_dir = sorted(listdir(self.dataset_root_dir))
        for i in range(0, len(all_datas_dir) - int(self.max_num_of_orms_to_retain)):
            purge_dir = join(self.dataset_root_dir, all_datas_dir[i])
            if exists(purge_dir):
                logger.info("Purging all the collected data dated [%s]" % purge_dir)
                try:
                    shutil.rmtree(purge_dir)
                except:
                    logger.error("Exception: %s" % (get_current_datetime(), sys.exc_info()))
            else:
                logger.error("Data dir [%s] not found" % purge_dir)

    def run(self):
        self.collect()
        if self.enable_upload is True:
            self.upsert_to_sfdc()
        if self.enable_auto_purge is True:
            self.purge_historical_datas()


def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")


def initialize_logging(log_dir=DEFAULT_LOGS_DIR):
    try:
        global logger
        if logger:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
        else:
            logger = logging.getLogger(__file__)
        DEFAULT_LOGS_DIR = app_root(log_dir)
        LOG_FILE = "%s/%s.log" % (DEFAULT_LOGS_DIR, splitext(basename(__file__))[0])
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(LOG_FILE)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except IOError as (errno, strerror):
        logger.error("Error: %s (%s): %s" % (log_dir, errno, strerror))
        sys.exit(1)
    except:
        logger.error("Unexpected error: %s" % sys.exc_info()[0])
        sys.exit(1)


def main():
    global DEFAULT_CONFIG_FILE, DEFAULT_DATA_DIR, DEFAULT_DB_DIR, DEFAULT_LOGS_DIR, logger
    start = time()
    chdir(dirname(dirname(__file__)))
    Utils().assure_path_exists(app_root(DEFAULT_LOGS_DIR))
    try:
        initialize_logging(log_dir=DEFAULT_LOGS_DIR)
        logger.info(banner)
        Sequelizer(DEFAULT_CONFIG_FILE).run()
    finally:
        if len(logger.handlers)>0:
            logger.info("Elapsed time for collecting %s report from %s is %s seconds" % (basename(splitext(__file__)[0]), Sequelizer(DEFAULT_CONFIG_FILE).cluster, time() - start))
            logger.info("***** Great! Mission Accomplished! *****")

if __name__ == "__main__":
    main()