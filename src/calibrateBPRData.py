#!/usr/bin/python
"""
Basic functions needed to calibrate ML-CORK / BPR data.
created by Martin Heesemann
modified by Angela Schlesinger march/April 2020

More text to describe what's going on
"""
import warnings
import numpy as np
import datetime
import sys, os



class CalibrationCoefficients(object):
    """ This class is to read the paroscientifc calibration coefficients based on
    a device IDs (pressure sensor serial number). The parosci.txt file needs to be
    in the same folder as this program."""
   
    def __init__(
         self, 
           ID=0x85,  # ID for BPR in hexformat
           
           
           parofile =os.path.join(os.path.dirname(os.path.abspath(__file__)),'parosci.txt'),
           thermsfile = os.path.join(os.path.dirname(os.path.abspath(__file__)),'therms.txt'),
           platinumfile = os.path.join(os.path.dirname(os.path.abspath(__file__)),'platinum.txt')):
           
           self.ID = ID
           print(self.ID)
           self.parofile = parofile
           self.thermsfile = thermsfile
           self.platinumfile = platinumfile
           


    def readParoCoeffs(self):
        """
        Read in the paroscientific calibration coefficients for a specific deviceID.
        Read coeff from parosci.txt file for pressure calibration coefficients.
        The files parosci.txt and platinum.txt, therms.txt 
        have to be in the same folder as this program    """
        ParoCoeffs={}
        f=open(self.parofile)
        N=0
        while True:
            try:
                N += 1
                line=f.readline()
                
                ParoID=int(line.split()[0])
            except:
                 print("Done!!!!!!!!!!!")
                 break
            IDCoeffs={}
            for i in range(1,12):
                line=f.readline().split()
                #print(line)
                IDCoeffs[line[0]]=float(line[1])
                ParoCoeffs[ParoID]=IDCoeffs

        f.close()
        return ParoCoeffs

   
    def getParoCoeffs(self,sensorId):
        """ return the specific calibration coefficients for a sensorID"""
        ParoCoeffs=self.readParoCoeffs()
        try:
            # print readParoCoeffs()[sensorId]
            ID=sensorId
            return ParoCoeffs[sensorId]
        except KeyError:
            print(Exception, 'You have to supply a valid probe serial # !!!!')
#            
#    
    
    def readPlatinumCoeffs(self):
        """
        Read in the platinum calibration coefficients for a specific deviceID.
        Read coeff from platinum.txt file for calibration coefficients.
        The files parosci.txt and platinum.txt, therms.txt 
        have to be in the same folder as this program    """
       
        
        PlatinumCoeffs={}
        f=open(self.platinumfile)
        N=0
        while True:
            try:
                N += 1
                print('======= ', N)
                line=f.readline()
                PlatID=int(line.split()[0],16) # Hex id
                line1=f.readline().split()
                line2=f.readline().split()
                print(PlatID)
            except:
                 print("Done!!!!!!!!!!!")
                 break
    
            PlatinumCoeffs[PlatID]={'a': float(line1[1]), 'b': float(line2[1])}
            print('%X' % PlatID, PlatinumCoeffs[PlatID])
        f.close()

        return PlatinumCoeffs


    def getPlatinumCoeffs(self, sensorId):
        """ return the specific calibration coefficients for a sensorID"""
        PlatinumCoeffs=self.readPlatinumCoeffs()
        try:
            return PlatinumCoeffs[sensorId]
        except KeyError:
            print(sensorId)
            warnings.warn('Using default temperature calibration!!!!')
            print('Cannot find calibation for platinum chip %X !!!!' % sensorId)
            return {'a': -2.93721e-006, 'b' : 40.0678 } # What is defaults ?? {'a': -2.95083e-006, 'b' : 40.0678 }
    
    
    def getParoIdList(self):
        """ return list of all ParoCoeff IDs"""
        ParoCoeffs = self.readParoCoeffs()
        IDs=ParoCoeffs.keys()
        print(IDs)
#        IDs.sort()
        return IDs
    
    def calibrateParoT(self,xFT,Coeffs=None):
        """Calibrate temperatures from Paroscientifc Type-II gauges"""
        
        if xFT==0:
            return np.nan;
    
        C=Coeffs
        U=((xFT+4294967296)*4.656612873e-9/4)-C['U0']
        return C['Y1']*U+C['Y2']*np.power(U,2)
    
    
    def calibratePlatinum(self,xT,Coeffs=None):
        """Calibrate temperatures from platinum chip sensors.
    
        The conversion from A/D counts to temperatures in degC is a simple linear relation ship.
    
        .. math::
    
           T = a \cdot x + b
        """
        C=Coeffs
        return C['a']*xT+C['b']
    
    def calibrateThermistor(self,xR,Coeffs=None):
        """Calibrate temperatures from thermistor.
        """
        C=Coeffs
        # Compute resistivities from counts
        R=C['x3']*(xR-C['x1'])/(C['x2']-C['x4']*(xR-C['x1']))
        # Do boilerplate Steinhart and Hart
        lnR=np.log(R)
        invT=C['a']+C['b']*lnR+C['c']*(lnR**3)
        Traw=1/invT-273.15
        # Do linear correction "calibration"
        slope=(C['x5']-C['x6'])/(C['x6']-25)
        return Traw+slope*(Traw-25)
    
    
    def calibrateParoP(self,xFP,Coeffs=None,xFT=None, Temp=None):
        if xFP==0:
            return np.nan;
    
        C=Coeffs
        isTypeI= C['U0']==0 # U0 is not zero for Type II probes
    
        if xFT==None and Temp==None:
            # Make sure some kind of compensation is done
            print('Using constant temperature 0 degC !!!!')
            Temp=0
            xFT=0
    
        if not isTypeI:
            # Compute Type-II compensation frequency U
            if xFT==None:
                # Compute compensation frequency if temperature is given for Type-II probes
                U= -(C['Y1']+np.sqrt(np.power(C['Y1'],2)+4*C['Y2']*Temp))/(2*C['Y2'])
            else:
                # Raw temperature freq count to compensate freq U
                U=(((xFT+4294967296)*4.656612873e-9)/4)-C['U0'];
        else:
            # For Type-I compensate (U) with temperature
            if Temp==None:
                # Make sure some kind of compensation is done
                warn('Using constant temperature 0 degC !!!!')
                Temp=0
            U=Temp
    
        # Do the proper compensation with U for pressure period
        T= (xFP+4294967296)*4.656612873e-9;
        CU= C['C1']+ C['C2']*U +C['C3'] * np.power(U,2);
        D= C['D1'];
        T0= C['T1'] + C['T2']*U + C['T3'] * np.power(U,2) + C['T4'] * np.power(U,3);
        P= CU * (1- (np.power(T0,2) / np.power(T,2))) * (1-D*(1- (np.power(T0,2) / np.power(T,2))));
        return P*6.894757/10 # Pressure in decibar
    
    def calibratePPCTime(self,xt=0x2A4E2328):
        # convert and calibrate (TODO) PPC time counts to python datetimes
        if not getattr(xt,'__iter__',False):
            t=datetime.datetime(1988,1,1)+datetime.timedelta(seconds=int(xt))
        else:
            t=[datetime.datetime(1988,1,1)+datetime.timedelta(seconds=int(Secs)) for Secs in xt]
        return t



