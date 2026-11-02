"""Named foreign-only scopes, kept explicit per species and historical series."""
def get_scope(name=None):
    countries=['DK','DE','PL','LT','LV','EE']
    if name is None or name=='southern-front':
        return dict(name='southern-front',countries=countries,latitude_range=[53,60],label='Södra grannlandsfronten · 53–60° N')
    if name=='baltic-finland':
        return dict(name=name,countries=countries+['FI'],latitude_range=[53,70],label='Finland och grannlandsfronten · 53–70° N')
    raise ValueError('Unreviewed occurrence scope')
