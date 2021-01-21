import pandas as pd
import requests
import json
import locale
from tqdm import tqdm
locale.setlocale(locale.LC_ALL, '')
#def _add_geodata(self, df):
def BBR(adresseid):
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
