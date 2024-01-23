FROM nginx:latest

COPY ./docker/nginx/default.conf /etc/nginx/conf.d/default.conf
COPY ./docker/nginx/.htpasswd.template /etc/nginx/.htpasswd
COPY ./docker/nginx/index.html /home/www/index.html
COPY ./docker/nginx/style.css /home/www/style.css



EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]