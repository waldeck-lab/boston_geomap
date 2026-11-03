"""Shared policy for private lists, their map data, CSVs and saved views."""
from werkzeug.exceptions import Forbidden

EXPIRED='Gratisperioden för privata artlistor har gått ut. Dina listor är sparade. Betaltjänsten kostar 50 kr/månad eller 250 kr/år. Betalregistreringen är inte öppnad ännu.'

def allowed(auth):
    user=auth.get('user') or {}
    return bool(auth.get('authenticated') and (user.get('is_admin') or (user.get('list_access') or {}).get('allowed') is True))

def require_personal_access(auth):
    if not allowed(auth):raise Forbidden(EXPIRED)
