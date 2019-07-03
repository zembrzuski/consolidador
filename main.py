from service import balancos_importer


def main():
    result = balancos_importer.importa_balancos('009342')
    print(result)


if __name__ == '__main__':
    main()
