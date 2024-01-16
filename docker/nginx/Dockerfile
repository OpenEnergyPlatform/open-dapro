FROM nginx:latest

COPY ./docker/nginx/default.conf /etc/nginx/conf.d/default.conf
COPY ./docker/nginx/.htpasswd /etc/nginx/.htpasswd


EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]