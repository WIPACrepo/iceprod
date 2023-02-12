

ROLES = {
    'admin': ['groups=/tokens/IceProdAdmins'],
    'user': ['groups=/institutions/IceCube.*'],
    'system': ['resource_access.iceprod.roles=iceprod-system'],
}

GROUPS = {
    'admin': ['groups=/tokens/IceProdAdmins'],
    'simprod': ['groups=/posix/simprod-submit'],
    'filtering': ['groups=/posix/i3filter'],
    'users': ['groups=/institutions/IceCube.*'],
}

GROUP_PRIORITIES = {
    'admin': 1.,
    'simprod': .5,
    'filtering': .9,
    'users': .7,
}
