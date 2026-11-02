# Étape 2 — Exploration de DummyJSON

## Objectif
Comprendre ce que renvoie l'API DummyJSON avant d'écrire le collecteur (étape 3) : tester les endpoints, repérer les champs utiles, définir les règles de normalisation.

## 1. Endpoints testés
```bash
curl "https://dummyjson.com/products?limit=0"
curl "https://dummyjson.com/users?limit=0"
curl "https://dummyjson.com/carts?limit=0"   # facultatif
```
`limit=0` renvoie tous les éléments en une seule réponse. Pagination possible avec `?limit=10&skip=20`.

Structure commune de la réponse :
```json
{ "products": [ ... ], "total": 194, "skip": 0, "limit": 194 }
```
Le tableau porte le nom de la ressource (`products`, `users`, `carts`).

| Ressource | `total` observé | Particularités |
|---|---|---|
| products | 194 | 24 catégories ; 92 produits sans `brand` |
| users | 208 | `address` est un objet imbriqué |
| carts | 208 | liste de produits avec `quantity` et `total` |

Les volumes peuvent changer : ne pas les écrire en dur dans le code. Les `id` sont uniques dans chaque ressource (utilisables comme clés primaires).

### Lire la sortie de curl
Le curl brut renvoie le JSON sur une seule ligne, illisible. Pour l'explorer :

- **Mettre en forme un seul élément** (sortie courte) :
  ```bash
  curl -s "https://dummyjson.com/products?limit=1" | python3 -m json.tool
  ```
  `-s` masque la barre de progression ; `limit=1` ne renvoie qu'un produit.
- **Lister uniquement les noms de champs** :
  ```bash
  curl -s "https://dummyjson.com/products?limit=1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(list(d['products'][0].keys()))"
  ```
- **Voir l'enveloppe sans les données** :
  ```bash
  curl -s "https://dummyjson.com/products?limit=0" | python3 -c "import sys,json; d=json.load(sys.stdin); print('total:', d['total'], '| skip:', d['skip'], '| limit:', d['limit'], '| reçus:', len(d['products']))"
  ```
- **Sauvegarder puis ouvrir dans VS Code** (formater avec `Maj+Alt+F`, replier les blocs avec les flèches) :
  ```bash
  curl -s "https://dummyjson.com/products?limit=0" -o products.json
  ```
  Ajouter `products.json` au `.gitignore` pour ne pas le publier.

Où regarder, dans cet ordre :
1. L'enveloppe : `total`, `skip`, `limit`.
2. Le premier élément du tableau (`products`, `users` ou `carts`) : ses clés sont la liste des champs possibles.
3. Les champs imbriqués (`{ }` ou `[ ]`) : `reviews`, `dimensions`, `meta` pour les produits, `address` pour les utilisateurs.

Commencer par `limit=1` sur chaque ressource pour obtenir un exemple complet de chaque structure.

## 2. Champs utiles
- **products** : `id`, `title`, `category`, `price`, `stock`, `brand` (+ `discountPercentage`, `rating`, `thumbnail` si utile au tableau de bord). Ignorer `reviews`, `images`, `meta`, `dimensions`, `weight`, `sku`.
- **users** : `id`, `firstName`, `lastName`, `email`, `address.city`. Ne pas stocker `password`, `ssn`, `bank`, `crypto`, `ip`, `macAddress` (principe de minimisation, pas de données sensibles dans Git).
- **carts** (facultatif) : `userId`, `products[].quantity`, pour calibrer la simulation.

## 3. Normalisation

| Source | Cible PostgreSQL | Règle |
|---|---|---|
| `products[].id` | `products.product_id` | clé primaire |
| `products[].title` / `category` / `price` / `stock` | mêmes noms | `price` décimal, `stock` entier |
| `products[].brand` | `products.brand` | `NULL` si absent (pas de valeur inventée) |
| `users[].id` | `customers.customer_id` | clé primaire |
| `users[].firstName` / `lastName` | `first_name` / `last_name` | passage en `snake_case` |
| `users[].email` | `customers.email` | |
| `users[].address.city` | `customers.city` | extrait de l'objet imbriqué |

Règles complémentaires :
- Rejeter un enregistrement sans `id` ou sans `price` (champ obligatoire manquant).
- Gérer les erreurs : timeout, code HTTP différent de 200, réponse incomplète (`total` différent du nombre d'éléments reçus).
- Tracer dans les logs : volumes attendus et reçus, erreurs, date de collecte.

## 4. Limites de l'API
- Données fictives ; les écritures ne sont pas persistées.
- Champs parfois absents (ex. `brand`).
- Les commandes et paiements ne viennent pas de l'API : ils sont simulés par le producteur.

## Livrables de l'étape
1. Cette note (endpoints, structure, particularités).
2. Le tableau de normalisation ci-dessus.
3. La liste des limites de l'API.
