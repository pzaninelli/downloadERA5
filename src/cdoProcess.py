import subprocess as subp
from os.path import exists as file_exists
import sys

class CDOProcess:
    
    __CMMD = ['copy', 'cat', 'mean', 'accumulated']
    __FREQ = ['day','month', 'season','year']
    __CDO_CMMD = {"copy":"copy", "cat": "cat", "mean": "mean", "accumulated": "sum",
                   'max': 'max', 'min': 'min', 'average': 'avg', 'range': 'range',
                   'std':'std', 'std1':'std1', 'var':'var', 'var1':'var1'}
    __CDO_FREQ = {'day':'day', 'month':'mon', 'season':'seas',  'year':'year'}
    
    
    def __init__(self, 
                 FileIn: str,
                 FileOut: str,
                 cmmd: str,
                 freq: str = None,
                 rem : bool = False) -> None:
        assert self._is_installed_CDO(), """
    CDO is not installed!!
        type 'sudo apt install cdo' for Debian, Ubuntu
        or visit https://code.mpimet.mpg.de/projects/cdo/files
        """
        self._filein = FileIn
        self._fileout = FileOut
        assert cmmd in self.__CMMD, "The statistic must be 'mean', 'accumulated', 'copy' or 'cat'"
        # assert freq in self.__FREQ, "The frequency must be 'day', 'month' or 'year'"
        self._freq = freq
        if cmmd=="copy" or cmmd=="cat":
            if not self._freq is None:
                raise ValueError("Frequency with commands 'copy' and 'cat' must be None!")
            self._cmmd = ['cdo', '-b', '32', self.__CDO_CMMD[cmmd], self._filein, self._fileout]    
        else:
            self._cmmd = ['cdo', '-b', '32', self.__CDO_FREQ[self._freq]+self.__CDO_CMMD[cmmd], self._filein, self._fileout]
        self._runnedproc = False
        self._stderr = None
        self._stdout = None
        self._rem = rem


    def __str__(self) -> str:
        return f"""
                    CDO Process:
                    {" ".join(self.cmmd)}
                        """
    
    @property
    def filein(self):
        return self._filein
    
    @property
    def fileout(self):
        return self._fileout
    
    @property
    def cmmd(self):
        return self._cmmd
    
    @property
    def is_runned(self):
        return self._runnedproc
    
    @property
    def stderr(self):
        return self._stderr
    
    @property
    def stdout(self):
        return self._stdout
    
    def run(self):
        proc = subp.run(self.cmmd)
        if not file_exists(self.fileout):
            raise FileExistsError(f"{self.fileout} was never created!")
        else:
            self._runnedproc = True
        try:
            self._stdout = proc.stdout.decode('utf-8')
        except:
            pass
        try:
            self._stderr = proc.stderr.decode('utf-8')
        except:
            pass
        if self._rem:
            self._remove_file(self.filein)
            
    @staticmethod
    def _remove_file(filename: str):
        comm=['rm', '-f', filename]
        if file_exists(filename):
            subp.run(comm)
            
    @staticmethod        
    def _is_installed_CDO():
        is_installed_cdo = subp.run(['which','cdo'],stdout=subp.PIPE, \
                                stderr=subp.PIPE)
        return is_installed_cdo.stdout.decode('utf-8') != ''

def concat_cdo(filein: list, fileout: str, remove_tempfiles : bool = False, verbose: bool = False) -> int:
    print(f"""
            *** Concatenate process starts to get file: {fileout} ***\n
          """)
    Cdo_copy = CDOProcess(filein[0], fileout, 'copy', rem=remove_tempfiles)
    if verbose:
        print(Cdo_copy)
    try:
        Cdo_copy.run()
    except FileExistsError:
        print("RuntimeError occurred!")
        sys.exit(1)
    for ii, ifile in enumerate(filein[1:]):
        Cdo = CDOProcess(ifile, fileout, 'cat', rem=remove_tempfiles)
        if verbose:
            print(Cdo)
        try:
            Cdo.run()     
        except FileExistsError:
            print(f"An error occurred for file: {ifile} iteration: {ii}")
            sys.exit(1)
    return 0
