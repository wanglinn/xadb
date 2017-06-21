#ifndef RDC_EXIT_H
#define RDC_EXIT_H

extern bool rdc_exit_inprogress;

typedef void (*rdc_on_exit_callback) (int code, Datum arg);

extern void rdc_exit(int code);
extern void on_rdc_exit(rdc_on_exit_callback function, Datum arg);

#endif	/* RDC_EXIT_H */
