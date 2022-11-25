# funzionde per restituire un dizionario
def makeDictFactory(cursor):
    ''' Funzione per creare un dizionario a partire da un cursore che deve essere passato come argomento
    
    E' definita all'interno di questa libreria e può essere usata da più script
    '''
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow