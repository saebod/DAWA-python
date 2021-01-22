import pandas as pd
import requests
import json
from tqdm import tqdm
def BBR(adresseid):
	""" For hver adresse ID fra DAWA indsamles oplysninger fra BBRlight
	Args:
		adresseid: Adresse ID fra DAWA"""
    tihi=[]
    r=requests.get('https://dawa.aws.dk/bbrlight/enheder?adresseid='+str(adresseid)).text
    j=json.loads(r)
    data={
    'Lej_Udlejningsforhold':j[0]['ENH_UDLEJ2_KODE'],
    'Lej_EnhedType':j[0]['ENH_ANVEND_KODE'],
    'Lej_ENERGI_KODE':j[0]['ENERGI_KODE'],
    'Lej_SagsType':j[0]['SagsType'],
    'Lej_AntalWc':j[0]['AntVandskylToilleter'],
    'Lej_AntalBadevaerelser':j[0]['AntBadevaerelser'],
    'Opgang_Elevator':j[0]['opgang']['Elevator'],
    'Opgang_SagsType':j[0]['opgang']['SagsType'],
    'Bygnings_aar':j[0]['bygning']['OPFOERELSE_AAR'],
    'Bygning_Ombygningaar':j[0]['bygning']['OMBYG_AAR'],
    'Bygning_Ydervaeg_kode':j[0]['bygning']['YDERVAEG_KODE'],
    'Bygning_VARMEINSTAL_KODE':j[0]['bygning']['VARMEINSTAL_KODE'],
    'Bygning_Tag_kode':j[0]['bygning']['TAG_KODE'],
    'Bygning_Antaletager':j[0]['bygning']['ETAGER_ANT'],
    'Bygning_ERHV_ARL_SAML':j[0]['bygning']['ERHV_ARL_SAML'],
    'Bygning_BOLIG_SAML':j[0]['bygning']['BYG_BOLIG_ARL_SAML'],
    'Bygning_Antal_Lejligheder':j[0]['bygning']['AntLejMKoekken'],
    'Bygning_SkadeForsikSelskab':j[0]['bygning']['BygSkadeForsikSelskab'],
    'Bygning_NyByg':j[0]['bygning']['NyByg'],
    'Bygning_SagsType':j[0]['bygning']['SagsType'],
    'Bygning_AndetAreal':j[0]['bygning']['ANDET_ARL'],
    'Bygning_KomEjerlavKode':j[0]['bygning']['KomEjerlavKode']}
    return data
def LejlighedData(df):
	""" bruges til at indsamle oplysninger både fra DAWA og BBRlight gennem en adresse
	Args:
		df: DataFrame hvor der er en kolonne der hedder adresse gerne så speficikt som muligt 
			dsv med postnummer og By
	 """
    url='https://dawa.aws.dk/adresser?q='
    for row in df.itertuples():
        try:
            r=requests.get(url+str(row.adresse)+str(',')).text
            j=json.loads(r)
            _BBR=BBR(j[0]['id'])
            df.at[row.Index,'lat']=str(j[0]['adgangsadresse']['adgangspunkt']['koordinater'][1]).replace('.',',')
            df.at[row.Index,'long']=(str(j[0]['adgangsadresse']['adgangspunkt']['koordinater'][0]).replace('.',','))
            df.at[row.Index,'KvmNet100M']=j[0]['adgangsadresse']['DDKN']['m100']
            df.at[row.Index,'KvmNet100M']=j[0]['adgangsadresse']['DDKN']['m100']
            df.at[row.Index,'KvmNet1km']=j[0]['adgangsadresse']['DDKN']['km1']
            df.at[row.Index,'KvmNet10km']=j[0]['adgangsadresse']['DDKN']['km10']
            df.at[row.Index,'Vejnavn']=j[0]['adgangsadresse']['vejstykke']['navn']
            df.at[row.Index,'husnr']=j[0]['adgangsadresse']['husnr']
            df.at[row.Index,'etage']=str(j[0]['etage'])
            df.at[row.Index,'door']=str(j[0]['dør'])
            df.at[row.Index,'postnummer']=j[0]['adgangsadresse']['postnummer']['nr']
            df.at[row.Index,'PostnummerBy']=j[0]['adgangsadresse']['postnummer']['navn']
            df.at[row.Index,'LandsdelNavn']=j[0]['adgangsadresse']['landsdel']['navn']
            df.at[row.Index,'LandsdelKode']=j[0]['adgangsadresse']['landsdel']['nuts3']
            df.at[row.Index,'Opstillingskreds']=j[0]['adgangsadresse']['opstillingskreds']['navn']
            df.at[row.Index,'Opstillingskredskode']=j[0]['adgangsadresse']['opstillingskreds']['kode']
            df.at[row.Index,'Lej_Udlejningsforhold']=_BBR['Lej_Udlejningsforhold']
            df.at[row.Index,'Lej_EnhedType']=_BBR['Lej_EnhedType']
            df.at[row.Index,'Lej_SagsType']=_BBR['Lej_SagsType']
            df.at[row.Index,'Lej_AntalWc']=_BBR['Lej_AntalWc']
            df.at[row.Index,'Lej_AntalBadevaerelser']=_BBR['Lej_AntalBadevaerelser']
            df.at[row.Index,'Opgang_Elevator']=_BBR['Opgang_Elevator']
            df.at[row.Index,'Opgang_SagsType']=_BBR['Opgang_SagsType']
            df.at[row.Index,'Bygnings_aar']=_BBR['Bygnings_aar']
            df.at[row.Index,'Bygning_Ombygningaar']=_BBR['Bygning_Ombygningaar']
            df.at[row.Index,'Bygning_Ydervaeg_kode']=_BBR['Bygning_Ydervaeg_kode']
            df.at[row.Index,'Bygning_VARMEINSTAL_KODE']=_BBR['Bygning_VARMEINSTAL_KODE']
            df.at[row.Index,'Bygning_Tag_kode']=_BBR['Bygning_Tag_kode']
            df.at[row.Index,'Bygning_Antaletager']=_BBR['Bygning_Antaletager']
            df.at[row.Index,'Bygning_ERHV_ARL_SAML']=_BBR['Bygning_ERHV_ARL_SAML']
            df.at[row.Index,'Bygning_BOLIG_SAML']=_BBR['Bygning_BOLIG_SAML']
            df.at[row.Index,'Bygning_Antal_Lejligheder']=_BBR['Bygning_Antal_Lejligheder']
            df.at[row.Index,'Bygning_SagsType']=_BBR['Bygning_SagsType']
            df.at[row.Index,'Bygning_AndetAreal']=_BBR['Bygning_AndetAreal']
            df.at[row.Index,'Bygning_KomEjerlavKode']=_BBR['Bygning_KomEjerlavKode']
        except:
            pass
    return df