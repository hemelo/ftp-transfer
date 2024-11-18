package com.hemelo.connect.utils;

import com.hemelo.connect.constants.Dates;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DateUtils {

    /**
     * Verifica se é meia-noite
     * @return boolean
     */
    public static boolean isMidnight() {
        LocalTime now = LocalTime.now(Dates.ZONE_ID);

        // Verifica se é meia-noite
        if (now.getHour() == 0 && now.getMinute() == 0) {
            return true;
        }

        return false;
    }

    /**
     * Retorna um cumprimento de acordo com o horário
     * @return
     */
    public static String getCumprimento() {
        LocalTime now = LocalTime.now(Dates.ZONE_ID);

        if (now.getHour() < 12) {
            return "Bom dia";
        } else if (now.getHour() < 18) {
            return "Boa tarde";
        } else {
            return "Boa noite";
        }
    }

    public static Set<LocalTime> generateHoursFromInterval(LocalTime startTime, LocalTime endTime, int n) {

        Duration piece = Duration.between(startTime, endTime).dividedBy(n);
        Set<LocalTime> horarios = IntStream.rangeClosed(0, n)
                .mapToObj(i -> startTime.plus(piece.multipliedBy(i))).collect(Collectors.toSet());

        horarios = horarios.stream().map(h -> {
            if (h.getSecond() == 59) {
                return h.plusSeconds(1);
            }

            return h;
        }).collect(Collectors.toSet());

        return horarios;
    }

    /**
     * Metodo para verificar se está trocando de dia
     * É utilizado para verificar situações críticas que devem ser executadas apenas uma vez por dia
     * @return
     */
    public static boolean isSwitchingDay() {
        LocalTime now = LocalTime.now(Dates.ZONE_ID);

        if ((now.getHour() == 23 && now.getMinute() >= 55) || isMidnight()) {
            return true;
        }

        return false;
    }
}
