-- name: GetNamespaceByName :one
SELECT * FROM namespaces WHERE name = $1;

-- name: CreateNamespace :one
INSERT INTO namespaces (name) VALUES ($1)
ON CONFLICT (name) DO NOTHING
RETURNING *;

-- name: ListNamespaces :many
SELECT * FROM namespaces ORDER BY name;
