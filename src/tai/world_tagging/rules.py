from .dsl import fuzzy, has_tag, one_of, regex, token

_rp_tokens = ['rp', 'рп', 'role', 'roleplay', fuzzy('roleplay'), fuzzy('ролеплау')]

rules_def = {}

rules_def['rp'] = (one_of(*_rp_tokens) | has_tag('city_rp')) & ~has_tag('dm')

rules_def['default'] = one_of(regex(r'World #\d+'), regex(r'Мир #\d+'))

rules_def['by_invite'] = token('pm')

rules_def['open'] = one_of('open', 'набор', regex(r'\[\d+/\d+\]'))

rules_def['in_dev'] = one_of(
    'dev',
    'делаю',
    'маппинг',
    'мапплю',
    'маппим',
    'в разработке',
    'разработка',
    'разраб',
    'coming soon',
    'wip',
    'скоро',
)

rules_def['dm'] = (
    one_of(
        fuzzy('deathmatch', 0.8),
        'dm',
        'uzi',
        'узи',
        'deagle',
        '+c',
        'cbug',
        'c-bug',
        regex(r'\+c arena'),
    )
    | has_tag('tdm')
    | has_tag('air')
    | has_tag('duel')
)

rules_def['tdm'] = (
    one_of(
        'tdm',
        'тдм',
        'vs',
        'против',
        'война',
        'war',
        'ready or not',
        'standoff',
        'team fortress',
    )
    | has_tag('ctf')
    | has_tag('gangwars')
)

rules_def['duel'] = one_of('duel', '1x1', '1v1', 'pvp', 'пвп', 'дуэль', 'дуэли')

rules_def['ctf'] = one_of('ctf', 'capture the flag', 'захват флага', 'битва за флаг')

rules_def['nsfw'] = one_of('sex', 'секс', 'rpsex', 'porno', 'porn')

rules_def['zombie'] = one_of(fuzzy('zombie', 0.8), 'зомби')

rules_def['copchase'] = one_of('copchase', 'копчейз', 'suspect')

rules_def['race'] = one_of(
    'race',
    fuzzy('racing'),
    'racer',
    'гонки',
    'nascar',
    'гонщики',
    'riders',
    'karmageddon',
    fuzzy('drift'),
    'дрифт',
    'hardcore drive',
    'initial d',
    'most wanted',
    'nfs',
    'raceworld',
    'rally',
    'ралли',
    'streetracing',
    'gonka',
) | has_tag('derby')

rules_def['derby'] = one_of('derby', 'дерби', 'derbi')

rules_def['minigames'] = (
    one_of(
        'agar io',
        'castle wars',
        'kitchen',
        'training pixel',
        'tags',
        'найди кнопку',
        'прятки',
        'fishing',
        'juggernaut',
        'джаггернаут',
    )
    | has_tag('spleef')
    | has_tag('stunt')
    | has_tag('clicker')
)

rules_def['stunt'] = one_of(
    fuzzy('parkour'), 'паркур', 'jump', 'стенка', 'bmx', 'stunt', 'skill test', 'спуск'
)
rules_def['spleef'] = one_of('spleef', 'сплиф')
rules_def['clicker'] = one_of('clicker', 'кликер')

rules_def['warsim'] = (
    one_of(
        'vietnam',
        'карабах',
        'франко прусская',
        'liga',
        'mil game',
        'chechnya',
        'northern war',
        'битва за британию',
    )
    | has_tag('ww2')
    | has_tag('svo')
    | has_tag('middle_east')
    | has_tag('tanks')
)

rules_def['tanks'] = one_of('tank', 'tanks', 'panzer', 'wot', 'танки', 'танковый')

rules_def['ww2'] = one_of('берлин', 'берлина', 'штурм рейхстага', '1945')
rules_def['svo'] = one_of(
    'avdoss', 'avdeevka', 'donbass', 'покровск', 'курск', 'курская', 'сво', 'odessa'
)
rules_def['middle_east'] = one_of('iraq', 'iran', 'ирак', 'афганистан', 'сирия')

rules_def['cops_robbers'] = one_of('cops', 'robbers')

rules_def['cops_vs_crime'] = one_of(
    'swat',
    'vs police',
    'jefferson',
    'копы',
    'crime',
    'преступники',
    'banditow',
    'бандитов',
    'копов',
)

rules_def['larp'] = one_of(
    'la',
    '1992',
    'lsrp',
    'los angeles',
    'dillimore',
    'la rp',
    'los santos',
    'ларп',
    'larp',
    'ls rp',
)

rules_def['chicago'] = one_of('chicago', 'chiraq')

rules_def['ghetto'] = (
    one_of('hoods', 'projects', 'ganton', 'ghetto rp', 'atlanta', 'detroit', fuzzy('ghetto'))
    | has_tag('larp')
    | has_tag('chicago')
)

rules_def['city_rp'] = (
    one_of(
        'lvrp',
        'lvrp',
        'new jersey',
        'sf rp',
        'нью джерси',
        'new jersy',
        'san fierro',
        'lv rp',
        'liberty city',
        'san fiero',
        'las vegas',
        'martlet city',
        'new york',
        'нью йорк',
    )
    | has_tag('larp')
    | has_tag('chicago')
    | has_tag('ghetto')
    | has_tag('new_jersey')
)

rules_def['new_jersey'] = token('new jersey')

rules_def['gangwars'] = one_of(
    'балас',
    'баласы',
    'vagos',
    'niggers',
    'негры',
    'вагос',
    'gangs',
    'банд',
    'aztecas',
    'мафия',
    'байкеры',
    'cuban',
    'haitian',
)

rules_def['sa_like'] = has_tag('kartel') | has_tag('robbing_sam') | has_tag('drugs_bombs')

rules_def['kartel'] = token('наркокартель')
rules_def['robbing_sam'] = token('robbing uncle sam')
rules_def['drugs_bombs'] = token('drugs n bombs')

rules_def['county_rp'] = one_of(
    'angel pine',
    'bayside',
    'county of ls',
    'ro la',
    'fc',
    'fcrp',
    'деревня',
    'округ',
    'county',
    'lone pine',
)

rules_def['prison'] = one_of('prison', 'alcatraz', 'jailbreak', 'дурка')

rules_def['post_apo'] = one_of(
    fuzzy('apocalypse', 0.8),
    'zone',
    fuzzy('stalker', 0.8),
    'twd rp',
    'dayz',
    'fallout',
    regex(r'S\.T\.A\.L\.K\.E\.R'),
    fuzzy('апокалипсис', 0.75),
)

rules_def['fnaf'] = token('fnaf')

rules_def['russia'] = one_of(
    'russia',
    'россия',
    'провинциальная',
    'провинция',
    fuzzy('санкт-петербург', 0.8),
    'питер',
    'piter',
    'санкт петербург',
    'moscow',
    'иркутск',
    'спб',
    'челябинск',
    'северозареченск',
)

rules_def['save_president'] = token('президент')
rules_def['air'] = one_of('гидр', 'гидры', 'air', 'гидрах', 'training flight')
rules_def['murder_mystery'] = token('murder mystery')
rules_def['college_rp'] = one_of('шарага', 'university', 'backyard')
rules_def['beta'] = token('beta')
rules_def['camp_rp'] = token('camp')
rules_def['farm'] = token('farmer')
rules_def['horror'] = one_of('granny', 'scp', fuzzy('horror', 0.8))

rules_def['thematic_rp'] = one_of(
    'history of brujas', 'squid online', 'star wars', 'долгопутье', 'squid'
)

rules_def['rpg'] = one_of(
    'pay 2 win',
    'правительство',
    'государство',
    'рыночные отношения',
    regex(r'г о с у д а р с т в о'),
)

rules_def['static_ads'] = one_of('chepotraining', 'wufus craft', 'че по тренингу')
rules_def['xviwar'] = one_of('xwivar', 'vivar', 'botwivar')

rules_def['movie'] = one_of('movie', 'film', 'cinema', 'studio', 'фильм', 'кино', 'съемки')
rules_def['anarchy'] = one_of('anarchy', 'anarhy', 'анархия', 'no rules', 'без правил')
rules_def['bum'] = one_of('bum', 'hobo', 'homeless', 'бомжатник', 'бутылки', 'бомж')

rules_def['party'] = one_of('party', 'disco', 'club', 'вечеринка', 'дискотека')
rules_def['personal_world'] = one_of(
    'denis', 'lexa', 'vlad', 'serik', 'rostik', 'house', 'дом', 'home'
)
