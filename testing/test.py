
players = {"ViRat":18, "RoHit":45, "SacHin":10, "Sky":63}

def test(**kwargs):
    for i , v in kwargs.items():
        print(i , v)

print(test(**players))