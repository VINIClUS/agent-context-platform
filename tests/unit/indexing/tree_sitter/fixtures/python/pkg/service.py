from . import models
from .models import Account as Acc, Ledger
import os.path, sys


def run() -> None:
    ledger = Ledger()
    ledger.total()
    Acc.build()
    models.helper("x")
    print(os.path.join("a", "b"))
    run()
