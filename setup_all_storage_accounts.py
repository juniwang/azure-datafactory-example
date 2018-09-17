from setup_storage_account import initialize_storage_account

known_storage_accounts = [
    ["rgsignalrprodcuseuap", "stsignalrprodcuseuap"],
    ["rgsignalrprodeastus", "stsignalrprodeastus"],
    ["rgsignalrprodseasia", "stsignalrprodseasia"],
    ["rgsignalrprodwestus", "stsignalrprodwestus"],
    ["rgsignalrprodwestus2", "stsignalrprodwestus2"],
    ["rgsignalrprodweu", "stsignalrprodweu"]
]

if __name__ == '__main__':
    [initialize_storage_account(x[0], x[1]) for x in known_storage_accounts]
