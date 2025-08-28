# gerar_chaves.py (versão definitiva com bcrypt)
import bcrypt

# Lista de senhas que você quer usar
senhas_em_texto_puro = ["Nelson@@1965", "Luisa@@@1965"]
hashed_passwords = []

print("--- Hashes Gerados com Sucesso ---")
print("Copie a lista completa abaixo (incluindo os colchetes []) e cole no seu arquivo config.yaml")

for senha in senhas_em_texto_puro:
    # Converte a senha de string para bytes
    bytes_senha = senha.encode('utf-8')
    
    # Gera o hash
    hashed = bcrypt.hashpw(bytes_senha, bcrypt.gensalt())
    
    # Converte o hash de bytes de volta para string para poder ser salvo
    hashed_passwords.append(hashed.decode('utf-8'))

# Imprime a lista final de hashes
print(hashed_passwords)