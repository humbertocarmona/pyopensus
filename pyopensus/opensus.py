# -*- coding: utf-8 -*-

import os
import ftplib
import datetime as dt
from ftplib import FTP 

class Opensus:
    '''
    
    '''
    def __init__(self) -> None:
        self._host = 'ftp.datasus.gov.br'
        self.baseftp = FTP(self._host)
        self.baseftp.login()
        self.basepath = '/dissemin/publicos/'
        
        self.sys_included = {'sihsus': self.basepath+'SIHSUS/200801_/Dados/', 
                             'siasus': self.basepath+'SIASUS/200801_/Dados/',
                             'sim': self.basepath+'SIM/CID10/DORES/',
                             'sinasc': self.basepath+'SINASC/1996_/Dados/DNRES/'}
        
        
        os.environ["R_HOME"] = os.path.join(os.environ["LOCALAPPDATA"], "Programs", "R", "R-4.3.1") # consolidate this better
        self.base_error = ftplib.all_errors[1]
        
    def reconnect_if_needed(self):
        try:
            self.baseftp.pwd()
        except self.base_error as err:
            self.baseftp = FTP(self._host)
            self.baseftp.login()
    
    # ---------------------------------------
    # -- hide host string from the interface.
    @property
    def host(self):
        return self.host
    
    @host.setter
    def host(self, v):
        raise Exception() # --
        
    def listdir(self, origin=None):
        '''
            List the files and folder of a specific directory within the host FTP.

            Args:
            -----
                origin:
                    String {default = None}. Name of the folder where to list the
                    files. If None, lists the files in the home folder.
        '''
        self.reconnect_if_needed()

        if origin is None:
            self.baseftp.cwd('/dissemin/publicos/')
            self.baseftp.retrlines('LIST')
        else:
            if origin.lower() in self.sys_included.keys():
                self.baseftp.cwd(self.sys_included[origin.lower()])
                self.baseftp.retrlines('LIST')

    def retrieve_year(self, dest, origin, uf, year, preffix="RD", verbose=False):
        '''
            Download data for a given year from one of the allowed sources.

            Args:
            -----
                dest:
                    String. Output folder.
                origin:
                    String. Source of the data.
                uf:
                    String. UF string for a given brazilian state.
                year:
                    Integer. Year referring to the desired data. 
                preffix:
                    String. Preffix string referring to the type of the data stored
                    for the system.
                verbose:
                    Bool. Verbose.
        '''
        self.reconnect_if_needed()

        this_year = dt.date.today().year
        if year < 2008 or year > this_year:
            raise Exception() # --
            
        if origin.lower() not in self.sys_included.keys():
            raise Exception() # --
        else:
            self.baseftp.cwd(self.sys_included[origin.lower()])
            
        year_str = f'{year}'[2:]
        filename = f"{preffix}{uf.upper()}{year_str}"
        month_lst = ['01', '02', '03', '04', '05', '06', '07',
                     '08', '09', '10', '11', '12']
            
        for cur_month in month_lst:
            filename_ = f'{filename}{cur_month}.dbc'
            if verbose:
                print(f'Download do arquivo {filename_} ...', end='')
                
            with open(os.path.join(dest, filename_), 'wb') as fp:
                self.baseftp.retrbinary(f'RETR {filename_}', fp.write)
                
            if verbose:
                print(' Feito.')

    def dbctodbf(self):
        '''
        
        '''
        pass