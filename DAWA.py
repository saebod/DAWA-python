import pandas as pd
import requests
import json
import locale
from tqdm import tqdm
from BBRLight import BBR
locale.setlocale(locale.LC_ALL, '')
#def _add_geodata(self, df):


df = pd.read_csv (r'chunk1.csv') 
_id=[]
_lat=[]
_long=[]
_KvmNet100M=[]
_KvmNet1km=[]
_KvmNet10km=[]
_etage=[]
_door=[]
_Vejnavn=[]
_husnr=[]
_postnummer=[]
_PostnummerBy=[]
_kommune=[]
_ejerlav=[]
_sogn=[]
_region=[]
_LandsdelNavn=[]
_LandsdelKode=[]
_Opstillingskreds=[]
_Opstillingskredskode=[]
_Lej_Udlejningsforhold=[]
_Lej_EnhedType=[]
_Lej_ENERGI_KODE=[]
_Lej_SagsType=[]
_Lej_AntalWc=[]
_Lej_AntalBadevaerelser=[]
_Opgang_Elevator=[]
_Opgang_SagsType=[]
_Bygnings_aar=[]
_Bygning_Ombygningaar=[]
_Bygning_Ydervaeg_kode=[]
_Bygning_VARMEINSTAL_KODE=[]
_Bygning_Tag_kode=[]
_Bygning_Antaletager=[]
_Bygning_ERHV_ARL_SAML=[]
_Bygning_BOLIG_SAML=[]
_Bygning_Antal_Lejligheder=[]
_Bygning_SkadeForsikSelskab=[]
_Bygning_NyByg=[]
_Bygning_SagsType=[]
_Bygning_AndetAreal=[]
_Bygning_KomEjerlavKode=[]

i=0
x=0
url='https://dawa.aws.dk/adresser?q='

for row in tqdm(df.itertuples()):
	try:
		r=requests.get(url+str(row.adresse)+str(',')).text
		j=json.loads(r)
		_id.append(j[0]['id'])
		_BBR=BBR(j[0]['id'])
		_long.append(str(j[0]['adgangsadresse']['adgangspunkt']['koordinater'][0]).replace('.',','))
		_lat.append(str(j[0]['adgangsadresse']['adgangspunkt']['koordinater'][1]).replace('.',','))
		_KvmNet100M.append(j[0]['adgangsadresse']['DDKN']['m100'])
		_KvmNet1km.append(j[0]['adgangsadresse']['DDKN']['km1'])
		_KvmNet10km.append(j[0]['adgangsadresse']['DDKN']['km10'])
		_Vejnavn.append(j[0]['adgangsadresse']['vejstykke']['navn'])
		_husnr.append(j[0]['adgangsadresse']['husnr'])
		_etage.append(j[0]['etage'])
		_door.append(j[0]['dør'])
		_postnummer.append(j[0]['adgangsadresse']['postnummer']['nr'])
		_PostnummerBy.append(j[0]['adgangsadresse']['postnummer']['navn'])
		_LandsdelNavn.append(j[0]['adgangsadresse']['landsdel']['navn'])
		_LandsdelKode.append(j[0]['adgangsadresse']['landsdel']['nuts3'])
		_Opstillingskreds.append(j[0]['adgangsadresse']['opstillingskreds']['navn'])
		_Opstillingskredskode.append(j[0]['adgangsadresse']['opstillingskreds']['kode'])
		_Lej_Udlejningsforhold.append(_BBR['Lej_Udlejningsforhold'])
		_Lej_EnhedType.append(_BBR['Lej_EnhedType'])
		_Lej_ENERGI_KODE.append(_BBR['Lej_ENERGI_KODE'])
		_Lej_SagsType.append(_BBR['Lej_SagsType'])
		_Lej_AntalWc.append(_BBR['Lej_AntalWc'])
		_Lej_AntalBadevaerelser.append(_BBR['Lej_AntalBadevaerelser'])
		_Opgang_Elevator.append(_BBR['Opgang_Elevator'])
		_Opgang_SagsType.append(_BBR['Opgang_SagsType'])
		_Bygnings_aar.append(_BBR['Bygnings_aar'])
		_Bygning_Ombygningaar.append(_BBR['Bygning_Ombygningaar'])
		_Bygning_Ydervaeg_kode.append(_BBR['Bygning_Ydervaeg_kode'])
		_Bygning_VARMEINSTAL_KODE.append(_BBR['Bygning_VARMEINSTAL_KODE'])
		_Bygning_Tag_kode.append(_BBR['Bygning_Tag_kode'])
		_Bygning_Antaletager.append(_BBR['Bygning_Antaletager'])
		_Bygning_ERHV_ARL_SAML.append(_BBR['Bygning_ERHV_ARL_SAML'])
		_Bygning_BOLIG_SAML.append(_BBR['Bygning_BOLIG_SAML'])
		_Bygning_Antal_Lejligheder.append(_BBR['Bygning_Antal_Lejligheder'])
		_Bygning_SkadeForsikSelskab.append(_BBR['Bygning_SkadeForsikSelskab'])
		_Bygning_NyByg.append(_BBR['Bygning_NyByg'])
		_Bygning_SagsType.append(_BBR['Bygning_SagsType'])
		_Bygning_AndetAreal.append(_BBR['Bygning_AndetAreal'])
		_Bygning_KomEjerlavKode.append(_BBR['Bygning_KomEjerlavKode'])
		i+=1
	except:
		x+=1
		print("fejl nummber "+str(x))
		_id.append('')
		_long.append('')
		_lat.append('')
		_KvmNet100M.append('')
		_KvmNet1km.append('')
		_KvmNet10km.append('')
		_Vejnavn.append('')
		_husnr.append('')
		_etage.append('')
		_door.append('')
		_postnummer.append('')
		_PostnummerBy.append('')
		_LandsdelNavn.append('')
		_LandsdelKode.append('')
		_Opstillingskreds.append('')
		_Opstillingskredskode.append('')
		pass
df['id']=_id
df["lat"]=_lat
df["long"]=_long
df['KvmNet100M']=_KvmNet100M
df['KvmNet1km']=_KvmNet1km
df['KvmNet10km']=_KvmNet10km
df['Vejnavn']=_Vejnavn
df['Husnr']=_husnr
df['Etage']=_etage
df['Door']=_door
df['Postnummer']=_postnummer
df['PostnummerBy']=_PostnummerBy
df['LandsdelNavn']=_LandsdelNavn
df['LandsdelKode']=_LandsdelKode
df['Opstillingskreds']=_Opstillingskreds
df['Opstillingskredskode']=_Opstillingskredskode
df['_Lej_Udlejningsforhold']=_Lej_Udlejningsforhold
df['_Lej_EnhedType']=_Lej_EnhedType
df['_Lej_ENERGI_KODE']=_Lej_ENERGI_KODE
df['_Lej_SagsType']=_Lej_SagsType
df['_Lej_AntalWc']=_Lej_AntalWc
df['_Lej_AntalBadevaerelser']=_Lej_AntalBadevaerelser
df['_Opgang_Elevator']=_Opgang_Elevator
df['_Opgang_SagsType']=_Opgang_SagsType
df['_Bygnings_aar']=_Bygnings_aar
df['_Bygning_Ombygningaar']=_Bygning_Ombygningaar
df['_Bygning_Ydervaeg_kode']=_Bygning_Ydervaeg_kode
df['_Bygning_VARMEINSTAL_KODE']=_Bygning_VARMEINSTAL_KODE
df['_Bygning_Tag_kode']=_Bygning_Tag_kode
df['_Bygning_Antaletager']=_Bygning_Antaletager
df['_Bygning_ERHV_ARL_SAML']=_Bygning_ERHV_ARL_SAML
df['_Bygning_BOLIG_SAML']=_Bygning_BOLIG_SAML
df['_Bygning_Antal_Lejligheder']=_Bygning_Antal_Lejligheder
df['_Bygning_SkadeForsikSelskab']=_Bygning_SkadeForsikSelskab
df['_Bygning_NyByg']=_Bygning_NyByg
df['_Bygning_SagsType']=_Bygning_SagsType
df['_Bygning_AndetAreal']=_Bygning_AndetAreal
df['_Bygning_KomEjerlavKode']=_Bygning_KomEjerlavKode

df.to_csv("chunk1AddedData.csv", encoding="utf-8-sig")